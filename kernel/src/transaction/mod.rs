use std::collections::{HashMap, HashSet};
use std::iter;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

use tracing::{info, instrument};
use url::Url;

use crate::actions::deletion_vector::DeletionVectorPath;
use crate::actions::{
    as_log_add_schema, generate_snapshot_id, get_commit_schema, get_log_commit_info_schema,
    get_log_content_root_schema, get_log_domain_metadata_schema, get_log_remove_schema,
    get_log_txn_schema, CommitInfo, ContentRoot, DomainMetadata, SetTransaction,
    INTERNAL_DOMAIN_PREFIX, METADATA_NAME, PROTOCOL_NAME,
};
use crate::committer::{CommitMetadata, CommitResponse, Committer};
use crate::content_tree::writer::ContentTreeNodeWriter;
use crate::engine_data::{FilteredEngineData, TypedGetData};
use crate::error::Error;
use crate::expressions::ColumnName;
use crate::expressions::{ArrayData, Transform, UnaryExpressionOp::ToJson};
use crate::path::{LogRoot, ParsedLogPath};
use crate::row_tracking::{
    RowTrackingDomainMetadata, RowTrackingVisitor, ROW_TRACKING_DOMAIN_NAME,
};
use crate::scan::data_skipping::stats_schema::NullableStatsTransform;
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, FILE_CONSTANT_VALUES_NAME, TAGS_NAME,
};
use crate::scan::scan_row_schema;
use crate::schema::{ArrayType, MapType, SchemaRef, StructField, StructType, StructTypeBuilder};
use crate::snapshot::SnapshotRef;
use crate::table_features::{ColumnMappingMode, Operation, TableFeature};
use crate::utils::{current_time_ms, require};
use crate::FileMeta;
use crate::{
    DataType, DeltaResult, Engine, EngineData, Expression, ExpressionRef, IntoEngineData,
    RowVisitor, SchemaTransform, Version, PRE_COMMIT_VERSION,
};
use delta_kernel_derive::internal_api;

pub mod leaf_writer;

// Re-export types needed for public API
pub use leaf_writer::LeafNodeWriterResult;

#[cfg(feature = "internal-api")]
pub mod builder;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod builder;

#[cfg(feature = "internal-api")]
pub mod create_table;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod create_table;

#[cfg(feature = "internal-api")]
pub mod data_layout;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod data_layout;

mod update;

/// Type alias for an iterator of [`EngineData`] results.
pub(crate) type EngineDataResultIterator<'a> =
    Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + 'a>;

/// The static instance referenced by [`add_files_schema`] that doesn't contain the dataChange column.
pub(crate) static MANDATORY_ADD_FILE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(vec![
        StructField::not_null("path", DataType::STRING),
        StructField::not_null(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, true),
        ),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("modificationTime", DataType::LONG),
    ]))
});

/// Returns a reference to the mandatory fields in an add action.
///
/// Note this does not include "dataChange" which is a required field but
/// but should be set on the transactoin level. Getting the full schema
/// can be done with [`Transaction::add_files_schema`].
pub(crate) fn mandatory_add_file_schema() -> &'static SchemaRef {
    &MANDATORY_ADD_FILE_SCHEMA
}

/// The base schema for add file metadata, referenced by [`Transaction::add_files_schema`].
///
/// The `stats` field represents the minimum structure. The actual stats written by
/// [`DefaultEngine::write_parquet`] include additional fields computed from the data:
/// - `nullCount`: nested struct mirroring the data schema (all fields LONG)
/// - `minValues`: nested struct with min/max eligible column types
/// - `maxValues`: nested struct with min/max eligible column types
///
/// The nested structures within nullCount/minValues/maxValues depend on the table's data schema
/// and which columns have statistics enabled. Use [`Transaction::stats_schema`] to get the
/// expected stats schema for a specific table.
///
/// [`DefaultEngine::write_parquet`]: crate::engine::default::DefaultEngine::write_parquet
pub(crate) static BASE_ADD_FILES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let stats = StructField::nullable(
        "stats",
        DataType::struct_type_unchecked(vec![
            StructField::nullable("numRecords", DataType::LONG),
            // nullCount, minValues, maxValues are dynamic based on data schema.
            // Empty struct placeholders indicate these fields exist but their inner
            // structure depends on the table schema and stats column configuration.
            StructField::nullable("nullCount", DataType::struct_type_unchecked(vec![])),
            StructField::nullable("minValues", DataType::struct_type_unchecked(vec![])),
            StructField::nullable("maxValues", DataType::struct_type_unchecked(vec![])),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]),
    );

    StructTypeBuilder::from_schema(mandatory_add_file_schema())
        .add_field(stats)
        .build_arc_unchecked()
});

static DATA_CHANGE_COLUMN: LazyLock<StructField> =
    LazyLock::new(|| StructField::not_null("dataChange", DataType::BOOLEAN));

/// Extend a schema with the dataChange column and return a new SchemaRef.
///
/// The dataChange column is inserted after the modificationTime field.
fn with_data_change_col(schema: &SchemaRef) -> SchemaRef {
    let mut fields = schema.fields().collect::<Vec<_>>();
    let len = fields.len();
    let insert_position = fields
        .iter()
        .position(|f| f.name() == "modificationTime")
        .unwrap_or(len);
    fields.insert(insert_position + 1, &DATA_CHANGE_COLUMN);
    Arc::new(StructType::new_unchecked(fields.into_iter().cloned()))
}

/// Extend a schema with a statistics column and return a new SchemaRef.
///
/// The stats column is of type string as required by the spec.
///
/// Note that this method is only useful to extend an Add action schema.
fn with_stats_col(schema: &SchemaRef) -> SchemaRef {
    StructTypeBuilder::from_schema(schema)
        .add_field(StructField::nullable("stats", DataType::STRING))
        .build_arc_unchecked()
}

/// Extend a schema with row tracking columns and return a new SchemaRef.
///
/// Note that this method is only useful to extend an Add action schema.
fn with_row_tracking_cols(schema: &SchemaRef) -> SchemaRef {
    StructTypeBuilder::from_schema(schema)
        .add_field(StructField::nullable("baseRowId", DataType::LONG))
        .add_field(StructField::nullable(
            "defaultRowCommitVersion",
            DataType::LONG,
        ))
        .build_arc_unchecked()
}

/// Marker type for transactions on existing tables.
///
/// This is the default state for [`Transaction`] and provides the full set of operations
/// including file removal, deletion vector updates, and blind append semantics.
#[derive(Debug)]
pub struct ExistingTable;

/// Marker type for create-table transactions.
///
/// Transactions in this state have a restricted API surface — operations that are semantically
/// invalid for table creation (e.g. file removal, domain metadata removal) are not available.
#[derive(Debug)]
pub struct CreateTable;

/// A transaction represents an in-progress write to a table. After creating a transaction, changes
/// to the table may be staged via the transaction methods before calling `commit` to commit the
/// changes to the table.
///
/// The type parameter `S` controls which operations are available:
/// - [`ExistingTable`] (default): Full API for modifying existing tables.
/// - [`CreateTable`]: Restricted API for table creation (see
///   [`CreateTableTransaction`](create_table::CreateTableTransaction)).
///
/// # Examples
///
/// ```rust,ignore
/// // create a transaction
/// let mut txn = table.new_transaction(&engine)?;
/// // stage table changes (right now only commit info)
/// txn.commit_info(Box::new(ArrowEngineData::new(engine_commit_info)));
/// // commit! (consume the transaction)
/// txn.commit(&engine)?;
/// ```
pub struct Transaction<S = ExistingTable> {
    span: tracing::Span,
    // The snapshot this transaction is based on. For create-table transactions,
    // this is a pre-commit snapshot with PRE_COMMIT_VERSION.
    read_snapshot: SnapshotRef,
    committer: Box<dyn Committer>,
    operation: Option<String>,
    engine_info: Option<String>,
    add_files_metadata: Vec<Box<dyn EngineData>>,
    remove_files_metadata: Vec<FilteredEngineData>,
    // NB: hashmap would require either duplicating the appid or splitting SetTransaction
    // key/payload. HashSet requires Borrow<&str> with matching Eq, Ord, and Hash. Plus,
    // HashSet::insert drops the to-be-inserted value without returning the existing one, which
    // would make error messaging unnecessarily difficult. Thus, we keep Vec here and deduplicate in
    // the commit method.
    set_transactions: Vec<SetTransaction>,
    // commit-wide timestamp (in milliseconds since epoch) - used in ICT, `txn` action, etc. to
    // keep all timestamps within the same commit consistent.
    commit_timestamp: i64,
    // User-provided domain metadata additions (via with_domain_metadata API).
    user_domain_metadata_additions: Vec<DomainMetadata>,
    // System-generated domain metadata (from transforms, e.g., clustering).
    // TODO(#1779): Currently only populated during CREATE TABLE. For inserts, row tracking
    // domain metadata is handled separately via `row_tracking_high_watermark` parameter in
    // `generate_domain_metadata_actions`. Consider unifying system domain handling.
    system_domain_metadata_additions: Vec<DomainMetadata>,
    // Domain names to remove in this transaction. The configuration values are fetched during
    // commit from the log to preserve the pre-image in tombstones.
    user_domain_removals: Vec<String>,
    // Whether this transaction contains any logical data changes.
    data_change: bool,
    // Whether this transaction should batch commit to the metadata tree
    batch_commit: bool,
    // Whether this transaction should be marked as a blind append.
    is_blind_append: bool,
    // Files matched by update_deletion_vectors() with new DV descriptors appended. These are used
    // to generate remove/add action pairs during commit, ensuring file statistics are preserved.
    dv_matched_files: Vec<FilteredEngineData>,
    // Aggregated manifest deletion vectors from all leaf writers
    aggregated_manifest_dvs: HashMap<String, roaring::RoaringTreemap>,
    // Aggregated unreconciled files from all leaf writers (detects conflicts)
    aggregated_unreconciled: HashSet<(String, crate::transaction::leaf_writer::DVUniqueId)>,
    // Aggregated root DV actions to remove from root DV manifest
    aggregated_root_dv_actions: HashSet<String>,
    // Leaf manifest entries to include in root
    leaf_manifests: Vec<crate::content_tree::ContentTreeNodeEntry>,
    // Snapshot ID for tracking info
    snapshot_id: i64,
    /// Whether the root has been released to the client via release_root_and_delta_actions().
    /// When true, leaf writers will not track root entries for removal.
    root_released: bool,
    /// Cached root manifest URL to avoid repeated lookups when creating leaf writers.
    /// Initialized lazily on first access.
    cached_root_manifest_url: std::cell::OnceCell<Option<Url>>,
    // Clustering columns from domain metadata. Only populated if the ClusteredTable feature is
    // enabled. Used for determining which columns require statistics collection.
    clustering_columns: Option<Vec<ColumnName>>,
    // PhantomData marker for transaction state (ExistingTable or CreateTable).
    // Zero-sized; only affects the type system.
    _state: PhantomData<S>,
}

impl<S> std::fmt::Debug for Transaction<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let version_info = if self.is_create_table() {
            "create_table".to_string()
        } else {
            format!("{}", self.read_snapshot.version())
        };
        f.write_str(&format!(
            "Transaction {{ read_snapshot version: {}, engine_info: {} }}",
            version_info,
            self.engine_info.is_some()
        ))
    }
}

// =============================================================================
// Methods only for existing-table transactions (constructors + restricted API)
// =============================================================================
impl Transaction {
    /// Create a new transaction from a snapshot for an existing table. The snapshot will be used
    /// to read the current state of the table (e.g. to read the current version).
    ///
    /// Instead of using this API, the more typical (user-facing) API is
    /// [Snapshot::transaction](crate::snapshot::Snapshot::transaction) to create a transaction from
    /// a snapshot.
    pub(crate) fn try_new_existing_table(
        snapshot: impl Into<SnapshotRef>,
        committer: Box<dyn Committer>,
        engine: &dyn Engine,
    ) -> DeltaResult<Self> {
        let read_snapshot = snapshot.into();

        // important! before writing to the table we must check it is supported
        read_snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Write)?;

        // Read clustering columns from snapshot (returns None if clustering not enabled)
        let clustering_columns = read_snapshot.get_clustering_columns(engine)?;

        let commit_timestamp = current_time_ms()?;

        let span = tracing::info_span!(
            "txn",
            path = %read_snapshot.table_root(),
            read_version = read_snapshot.version(),
        );

        Ok(Transaction {
            span,
            read_snapshot,
            committer,
            operation: None,
            engine_info: None,
            add_files_metadata: vec![],
            remove_files_metadata: vec![],
            set_transactions: vec![],
            commit_timestamp,
            user_domain_metadata_additions: vec![],
            system_domain_metadata_additions: vec![],
            user_domain_removals: vec![],
            data_change: true,
            batch_commit: false,
            is_blind_append: false,
            dv_matched_files: vec![],
            aggregated_manifest_dvs: HashMap::new(),
            aggregated_unreconciled: HashSet::new(),
            aggregated_root_dv_actions: HashSet::new(),
            leaf_manifests: vec![],
            snapshot_id: generate_snapshot_id(),
            root_released: false,
            cached_root_manifest_url: std::cell::OnceCell::new(),
            clustering_columns,
            _state: PhantomData,
        })
    }
}

// =============================================================================
// Shared methods available on ALL transaction types
// =============================================================================
impl<S> Transaction<S> {
    /// Consume the transaction and commit it to the table. The result is a result of
    /// [CommitResult] with the following semantics:
    /// - Ok(CommitResult) for either success or a recoverable error (includes the failed
    ///   transaction in case of a conflict so the user can retry, etc.)
    /// - Err(Error) indicates a non-retryable error (e.g. logic/validation error).
    #[instrument(
        parent = &self.span,
        name = "txn.commit",
        skip_all,
        fields(
            commit_version = self.get_commit_version(),
        ),
        err
    )]
    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<CommitResult<S>> {
        info!(
            num_add_files = self.add_files_metadata.len(),
            num_remove_files = self.remove_files_metadata.len(),
            num_dv_updates = self.dv_matched_files.len(),
        );
        // Step 1: Check for duplicate app_ids and generate set transactions (`txn`)
        // Note: The commit info must always be the first action in the commit but we generate it in
        // step 2 to fail early on duplicate transaction appIds
        // TODO(zach): we currently do this in two passes - can we do it in one and still keep refs
        // in the HashSet?
        let mut app_ids = HashSet::with_capacity(self.set_transactions.len());
        if let Some(dup) = self
            .set_transactions
            .iter()
            .find(|t| !app_ids.insert(&t.app_id))
        {
            return Err(Error::generic(format!(
                "app_id {} already exists in transaction",
                dup.app_id
            )));
        }

        self.validate_blind_append_semantics()?;

        // CDF check only applies to existing tables (not create table)
        // If there are add and remove files with data change in the same transaction, we block it.
        // This is because kernel does not yet have a way to discern DML operations. For DML
        // operations that perform updates on rows, ChangeDataFeed requires that a `cdc` file be
        // written to the delta log.
        if !self.is_create_table()
            && !self.add_files_metadata.is_empty()
            && !self.remove_files_metadata.is_empty()
            && self.data_change
        {
            let cdf_enabled = self
                .read_snapshot
                .table_configuration()
                .table_properties()
                .enable_change_data_feed
                .unwrap_or(false);
            require!(
                !cdf_enabled,
                Error::generic(
                    "Cannot add and remove data in the same transaction when Change Data Feed is enabled (delta.enableChangeDataFeed = true). \
                     This would require writing CDC files for DML operations, which is not yet supported. \
                     Consider using separate transactions: one to add files, another to remove files."
                )
            );
        }

        // Step 1: Generate SetTransaction actions
        let set_transaction_actions = self
            .set_transactions
            .clone()
            .into_iter()
            .map(|txn| txn.into_engine_data(get_log_txn_schema().clone(), engine));

        // Step 2: Construct commit info with ICT if enabled
        let commit_info = CommitInfo::new(
            self.commit_timestamp,
            self.get_in_commit_timestamp(engine)?,
            self.operation.clone(),
            self.engine_info.clone(),
            self.snapshot_id,
            self.is_blind_append,
        );
        // Use transaction's snapshot_id directly (already i64)
        let snapshot_id = self.snapshot_id;
        let commit_info_action =
            commit_info.into_engine_data(get_log_commit_info_schema().clone(), engine);

        // Step 3: Get commit version for actions
        let commit_version = self.get_commit_version();

        // Step 4: Generate DV update actions (remove/add pairs) if any DV updates are present
        // TODO: In batch commit mode, DV updates should be recorded in the content tree rather
        // than written to the delta log (same issue as removes). This requires:
        // 1. Processing dv_matched_files in the batch commit block of generate_log_actions
        //    to update the content tree with new DV descriptors.
        // 2. Suppressing dv_update_actions from the log (similar to how remove_actions are
        //    suppressed when batch_commit is active).
        // 3. Including !self.dv_matched_files.is_empty() in is_batch_commit_active()'s
        //    has_work_to_do check.
        let dv_update_actions = self.generate_dv_update_actions(engine)?;

        // Step 5: Generate remove actions for the delta log (skipped in batch commit mode, where
        // removes are recorded in the content tree instead).
        let batch_commit = self.is_batch_commit_active();
        let remove_actions = if batch_commit {
            None
        } else {
            Some(self.generate_remove_actions(engine, self.remove_files_metadata.iter(), &[])?)
        };

        // Step 6: Generate all log actions (commit info, protocol, metadata for create-table,
        // set transactions, domain metadata, add actions)
        let actions = self.generate_log_actions(
            engine,
            commit_version,
            snapshot_id,
            commit_info_action,
            set_transaction_actions,
        )?;

        let filtered_actions = actions
            .into_iter()
            .chain(remove_actions.into_iter().flatten())
            .chain(dv_update_actions);

        // Step 7: Commit via the committer
        // Block FileSystemCommitter for catalog-managed tables (including create-table with catalog features)
        #[cfg(feature = "catalog-managed")]
        if !self.committer.is_catalog_committer()
            && self
                .read_snapshot
                .table_configuration()
                .is_catalog_managed()
        {
            return Err(Error::generic(
                "A catalog committer must be used to commit to catalog-managed tables. Please \
                    provide a catalog committer via Snapshot::transaction().",
            ));
        }
        let log_root = LogRoot::new(self.read_snapshot.table_root().clone())?;
        let commit_metadata = CommitMetadata::new(
            log_root,
            commit_version,
            self.commit_timestamp,
            self.read_snapshot.log_segment().max_published_version,
        );
        match self
            .committer
            .commit(engine, Box::new(filtered_actions), commit_metadata)
        {
            Ok(CommitResponse::Committed { file_meta }) => Ok(CommitResult::CommittedTransaction(
                self.into_committed(file_meta)?,
            )),
            Ok(CommitResponse::Conflict { version }) => Ok(CommitResult::ConflictedTransaction(
                self.into_conflicted(version),
            )),
            // TODO: we may want to be more or less selective about what is retryable (this is tied
            // to the idea of "what kind of Errors should write_json_file return?")
            Err(e @ Error::IOError(_)) => {
                Ok(CommitResult::RetryableTransaction(self.into_retryable(e)))
            }
            Err(e) => Err(e),
        }
    }

    /// Generate all JSON actions for the commit, including commit info, set transactions,
    /// domain metadata, and add actions (or content root for batch commits).
    fn generate_log_actions(
        &self,
        engine: &dyn Engine,
        commit_version: u64,
        snapshot_id: i64,
        commit_info_action: DeltaResult<Box<dyn EngineData>>,
        set_transaction_actions: impl Iterator<Item = DeltaResult<Box<dyn EngineData>>>,
    ) -> DeltaResult<Vec<DeltaResult<FilteredEngineData>>> {
        // Step 3: Generate add actions and get data for domain metadata actions (e.g. row tracking high watermark)
        let (add_actions, row_tracking_domain_metadata) =
            self.generate_adds(engine, commit_version)?;

        let domain_metadata_actions =
            self.generate_domain_metadata_actions(engine, row_tracking_domain_metadata)?;

        // Start with commit info action
        let mut actions_vec =
            vec![commit_info_action.map(FilteredEngineData::with_all_rows_selected)];

        // For create-table: add Protocol and Metadata actions after commit info
        if self.is_create_table() {
            let table_config = self.read_snapshot.table_configuration();
            let protocol = table_config.protocol().clone();
            let metadata = table_config.metadata().clone();

            let protocol_schema = get_commit_schema().project(&[PROTOCOL_NAME])?;
            let metadata_schema = get_commit_schema().project(&[METADATA_NAME])?;

            let protocol_data = protocol.into_engine_data(protocol_schema, engine)?;
            let metadata_data = metadata.into_engine_data(metadata_schema, engine)?;

            actions_vec.push(Ok(FilteredEngineData::with_all_rows_selected(
                protocol_data,
            )));
            actions_vec.push(Ok(FilteredEngineData::with_all_rows_selected(
                metadata_data,
            )));
        }

        actions_vec.extend(
            set_transaction_actions
                .map(|action| action.map(FilteredEngineData::with_all_rows_selected)),
        );
        actions_vec.extend(
            domain_metadata_actions
                .map(|action| action.map(FilteredEngineData::with_all_rows_selected)),
        );

        // Handle batch commit - either write to metadata tree or include in JSON log
        if self.is_batch_commit_active() {
            // Content metadata trees require column mapping mode to be ID for stable field IDs
            let column_mapping_mode = self
                .read_snapshot
                .table_configuration()
                .column_mapping_mode();
            require!(
                column_mapping_mode == crate::table_features::ColumnMappingMode::Id,
                Error::generic(format!(
                    "Content metadata trees (batch_commit mode) require column mapping mode 'id', found '{:?}'",
                    column_mapping_mode
                ))
            );

            // Get the cached content root from the snapshot (no I/O needed)
            let latest_content_root = self
                .read_snapshot
                .content_root()
                .map(|cr| (cr.clone(), cr.version));

            let table_schema = self.read_snapshot.schema().as_ref().clone();
            // Convert to physical schema with PARQUET:field_id metadata for stats mapping
            let physical_table_schema = table_schema.make_physical(column_mapping_mode);
            let table_root = self.read_snapshot.table_root().clone();
            let current_version = self.read_snapshot.version();

            // Load existing metadata and determine the version from which to replay delta log
            let (mut metadata_builder, root_manifest_path, replay_from_version) =
                if let Some((content_root_action, content_root_version)) = latest_content_root {
                    // Load metadata from content root directly into the builder
                    let root_path = content_root_action.path.clone();
                    let builder =
                        crate::content_tree::builder::ContentTreeNodeBuilder::from_content_root(
                            engine,
                            &content_root_action,
                            table_root.clone(),
                            physical_table_schema.clone(),
                            commit_version,
                        )?;
                    // Replay delta log from the version after the content root
                    (builder, Some(root_path), content_root_version + 1)
                } else {
                    // No content root found, start with empty metadata
                    // Use commit_version for the new metadata, not the current snapshot version
                    let builder = crate::content_tree::builder::ContentTreeNodeBuilder::new_for(
                        table_root.clone(),
                        commit_version,
                        physical_table_schema.clone(),
                    );
                    // Replay all delta log commits from the beginning
                    (builder, None, 0)
                };

            // If root was released to client control, clear all root data and DV entries
            // The client will add them back via leaf manifests
            if self.root_released {
                metadata_builder.clear_root_data_and_dv_entries();

                // TODO: Process incremental removes from delta log and mark them as DELETED
                // in the appropriate leaf manifests. This requires:
                // 1. Scanning delta log for Remove actions since the content root version
                // 2. Looking up which leaf manifest each removed file is in (via manifest metadata)
                // 3. Calling metadata_builder.delete_from_leaf() for each removed file
                // This is deferred to future work as it requires a new delta log processor.
            } else if replay_from_version <= current_version {
                // Root not released: replay delta log commits to add incremental changes
                // Create a scan of just root + delta log (skip leaves to avoid duplicates)
                let scan = crate::scan::ScanBuilder::new(self.read_snapshot.clone())
                    .skip_leaf_manifests(true)
                    .build()?;
                let scan_metadata_iter = scan.scan_metadata(engine)?;

                for scan_metadata_result in scan_metadata_iter {
                    let scan_metadata = scan_metadata_result?;
                    let engine_data = scan_metadata.scan_files.data();

                    // Add incremental actions from delta log to the metadata builder
                    // TODO: When replaying, we should preserve original sequence_numbers from the
                    // files' tracking_info instead of using current_version. This would require
                    // extracting sequence_number from the scan data and passing it through.
                    metadata_builder.add_from_scan_row_data(
                        engine_data,
                        current_version,
                        snapshot_id,
                    )?;
                }
            }

            for add_metadata_result in self.add_files_metadata.iter() {
                // Pre-convert stats from Delta JSON format to AMT struct format at batch level
                let converted = crate::content_tree::stats::try_pre_convert_stats_column(
                    engine,
                    add_metadata_result.as_ref(),
                    "stats",
                    &physical_table_schema,
                    &BASE_ADD_FILES_SCHEMA,
                )?;
                let data: &dyn EngineData = match &converted {
                    Some(c) => c.as_ref(),
                    None => add_metadata_result.as_ref(),
                };
                metadata_builder.add_from_engine_data_write(
                    engine,
                    data,
                    commit_version,
                    snapshot_id,
                )?;
            }

            // Add leaf manifests collected via add_leaf() to the ContentRoot
            for leaf_manifest_entry in &self.leaf_manifests {
                metadata_builder.add_entry(leaf_manifest_entry.clone());
            }

            // Remove files that were moved to leaves from the root manifest entirely
            // This handles files that lacked metadata when added to leaf
            for (file_path, _) in &self.aggregated_unreconciled {
                metadata_builder.remove_data_file(file_path.as_str())?;
            }

            // Remove DVs that were moved to leaves from the root DV manifest
            for dv_path in &self.aggregated_root_dv_actions {
                metadata_builder.remove_dv(dv_path.as_str())?;
            }

            // Apply aggregated manifest DVs from leaf writers
            // These mark entries in leaf manifests as deleted (e.g., when files are moved between leaves)
            // set_changes_dv=false because this is leaf reorganization, not actual user-facing deletion
            for (manifest_path, entry_indices) in &self.aggregated_manifest_dvs {
                metadata_builder.delete_multiple_from_leaf(
                    manifest_path,
                    entry_indices,
                    false, // Don't set changes_dv for leaf reorganization
                )?;
            }

            // In batch mode, process ALL remove actions and mark entries as DELETED in the ContentRoot
            // The ContentRoot manages all file state, so any removes should be reflected there
            // This applies whether we loaded from an existing ContentRoot or built from snapshot
            if !self.remove_files_metadata.is_empty() {
                use crate::actions::visitors::RemoveVisitor;
                use crate::engine_data::GetData;
                use crate::RowVisitor;

                // Custom visitor that only collects selected Remove actions based on selection vector
                struct SelectionFilteredRemoveVisitor<'a> {
                    removes: Vec<crate::actions::Remove>,
                    selection_vector: &'a [bool],
                }

                impl<'a> SelectionFilteredRemoveVisitor<'a> {
                    fn new(selection_vector: &'a [bool]) -> Self {
                        Self {
                            removes: Vec::new(),
                            selection_vector,
                        }
                    }
                }

                impl<'a> RowVisitor for SelectionFilteredRemoveVisitor<'a> {
                    fn selected_column_names_and_types(
                        &self,
                    ) -> (&'static [ColumnName], &'static [DataType]) {
                        RemoveVisitor::names_and_types()
                    }

                    fn visit<'b>(
                        &mut self,
                        row_count: usize,
                        getters: &[&'b dyn GetData<'b>],
                    ) -> DeltaResult<()> {
                        for i in 0..row_count {
                            // Check if this row is selected (true if index >= selection_vector.len() or selection_vector[index] is true)
                            let is_selected =
                                i >= self.selection_vector.len() || self.selection_vector[i];
                            if !is_selected {
                                continue;
                            }

                            // Since path column is required, use it to detect presence of a Remove action
                            if let Some(path) = getters[0].get_opt(i, "remove.path")? {
                                let remove = RemoveVisitor::visit_remove(i, path, getters)?;
                                self.removes.push(remove);
                            }
                        }
                        Ok(())
                    }
                }

                // Accumulate leaf deletions per manifest path to batch into a single
                // delete_multiple_from_leaf call per leaf (avoids repeated bitmap |= per file).
                let mut leaf_deletions: HashMap<String, roaring::RoaringTreemap> = HashMap::new();

                // Process each remove batch and mark all files as deleted in the ContentRoot
                for remove_action_result in
                    self.generate_remove_actions(engine, self.remove_files_metadata.iter(), &[])?
                {
                    let remove_action = remove_action_result?;
                    // Get the selection vector to pass to visitor
                    let selection_vector = remove_action.selection_vector();
                    let mut visitor = SelectionFilteredRemoveVisitor::new(selection_vector);
                    // Visit the data, filtering by selection vector during visit
                    visitor.visit_rows_of(remove_action.data())?;

                    // Mark all removes as deleted in the ContentRoot
                    for remove in visitor.removes.iter() {
                        // Check if this remove is in a leaf manifest (CombinedManifest).
                        // In the flat structure DVs are inline on the data entry, so there
                        // is no separate delete manifest — only data_manifest_path matters.
                        let data_in_leaf = remove
                            .data_manifest_path
                            .as_ref()
                            .filter(|path| {
                                // Check if path is not the root manifest
                                root_manifest_path.as_ref() != Some(path)
                            })
                            .is_some()
                            && remove.data_manifest_position.is_some();

                        // Accumulate leaf manifest deletions to batch later
                        if data_in_leaf {
                            if let (Some(manifest_path), Some(position)) = (
                                remove.data_manifest_path.as_ref(),
                                remove.data_manifest_position,
                            ) {
                                leaf_deletions
                                    .entry(manifest_path.clone())
                                    .or_default()
                                    .insert(position as u64);
                            }
                        }

                        // Use mark_deleted for entries in root manifest (not in a leaf)
                        if !data_in_leaf {
                            let dv_path: Option<&str> = remove
                                .deletion_vector
                                .as_ref()
                                .map(|dv| dv.path_or_inline_dv.as_str());

                            metadata_builder.mark_deleted(
                                Some(remove.path.as_str()),
                                dv_path,
                                commit_version,
                                snapshot_id,
                            )?;
                        }
                    }
                }

                // Flush accumulated leaf deletions: one bulk call per leaf instead of one per file
                for (manifest_path, indices) in &leaf_deletions {
                    metadata_builder.delete_multiple_from_leaf(manifest_path, indices, true)?;
                }
            }

            let new_metadata = metadata_builder.build(engine, snapshot_id)?;
            let (content_metadata_path, size_in_bytes) =
                ContentTreeNodeWriter::try_new(new_metadata)?.write(engine)?;
            let path = crate::content_tree::absolute_to_relative_path(
                &content_metadata_path,
                self.read_snapshot.table_root(),
            )?;

            // The content root represents the state at the new commit version
            let new_commit_version = self.read_snapshot.version() + 1;
            let content_root_action = ContentRoot {
                path,
                size_in_bytes,
                version: new_commit_version,
            };

            // Use the log schema to wrap ContentRoot in a "contentRoot" field
            let content_root_data =
                content_root_action.into_engine_data(get_log_content_root_schema().clone(), engine);

            actions_vec.push(content_root_data.map(FilteredEngineData::with_all_rows_selected))
        } else {
            // Normal mode: add actions go in the JSON log
            // Remove actions are added separately in the commit method
            actions_vec.extend(
                add_actions.map(|action| action.map(FilteredEngineData::with_all_rows_selected)),
            );
        }

        Ok(actions_vec)
    }

    /// Set the data change flag.
    ///
    /// True indicates this commit is a "data changing" commit. False indicates table data was
    /// reorganized but not materially modified.
    ///
    /// Data change might be set to false in the following scenarios:
    /// 1. Operations that only change metadata (e.g. backfilling statistics)
    /// 2. Operations that make no logical changes to the contents of the table (i.e. rows are only moved
    ///    from old files to new ones.  OPTIMIZE commands is one example of this type of optimizaton).
    pub fn with_data_change(mut self, data_change: bool) -> Self {
        self.data_change = data_change;
        self
    }

    /// Set whether the transaction should use batch commit mode (commit directly to the metadata tree).
    /// When not set, the transaction writes incremental commits to the log.
    ///
    /// Any incremental actions accumulated since the last batch commit will automatically
    /// be added to the tree root on commit.
    ///
    /// Requires preview feature preview-adaptive-metadata-tree be turned on for the table.
    /// TODO: add the feature option.
    /// TODO: Add option to replay incremental actions and add them to leaves.
    pub fn with_batch_commit(mut self) -> Self {
        self.batch_commit = true;
        self
    }

    /// Returns a Scan that replays actions from both the root manifest (if present) and delta log.
    ///
    /// After calling this method, the transaction records that the root has been "released" to the
    /// client. Any subsequent LeafNodeWriter instances created via `new_leaf_node_writer()` will NOT
    /// track root entries for removal, since the client is now responsible for managing which
    /// actions move from root to leaves.
    ///
    /// This is useful for partition-aware compaction workflows where the client wants to:
    /// 1. Read all actions from root + delta log
    /// 2. Process and partition them according to custom logic
    /// 3. Write partitioned actions to leaf manifests
    /// 4. Commit the transaction with only the leaf manifests (root stays unchanged)
    ///
    /// # Returns
    ///
    /// A Scan that will return all Add actions from:
    /// - The root manifest (if present in the checkpoint) - entries where `dataManifestPath` is NULL
    /// - All delta log files since the checkpoint - entries where `dataManifestPath` is NULL
    ///
    /// Note: The scan explicitly excludes actions from leaf manifests (where `dataManifestPath` is non-NULL)
    /// using an internal skip mechanism.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let txn = snapshot.transaction(committer)?;
    /// txn = txn.with_batch_commit();
    ///
    /// // Get all actions from root + delta
    /// let scan = txn.release_root_and_delta_actions()?;
    ///
    /// // Process actions and partition them
    /// for action_batch in scan.scan_metadata(engine)? {
    ///     let actions = process_and_partition(action_batch)?;
    ///
    ///     // Write to leaf manifests
    ///     let mut leaf = txn.new_leaf_node_writer(engine)?;
    ///     for action in actions {
    ///         leaf.add_existing_actions(engine, action)?;
    ///     }
    ///     let leaf_result = leaf.finish(engine)?;
    ///     txn.add_leaf(leaf_result)?;
    /// }
    ///
    /// // Commit the transaction
    /// txn.commit(engine)?;
    /// ```
    pub fn release_root_and_delta_actions(&mut self) -> DeltaResult<crate::scan::Scan> {
        // Validation: must be in batch commit mode
        if !self.batch_commit {
            return Err(Error::generic(
                "release_root_and_delta_actions() requires batch_commit mode. Call with_batch_commit() first."
            ));
        }

        // Validation: can only be called once
        if self.root_released {
            return Err(Error::generic(
                "release_root_and_delta_actions() can only be called once per transaction",
            ));
        }

        // Mark root as released
        self.root_released = true;

        // TODO: we need custom replay here to:
        // 1. Add it any currently added/removed actions to the log replay.
        // 2. Do leaf book-keeping for incrementally add/removed files (primarily DV updates).
        //
        // Create a scan that ONLY reads root + delta log (excluding leaf manifests)
        // Include stats columns so that parsed stats are available for AMT leaf population
        let scan_builder = crate::scan::ScanBuilder::new(self.read_snapshot.clone());
        let scan = scan_builder
            .skip_leaf_manifests(true)
            .include_stats_columns()
            .build()?;

        Ok(scan)
    }

    /// Create a new leaf node writer for this transaction.
    ///
    /// The writer can be used to add files to a leaf manifest, which will be written
    /// and incorporated into the root manifest when the transaction commits.
    ///
    /// # Arguments
    /// * `engine` - The engine to use for fetching the root manifest URL (only on first call)
    ///
    /// # Returns
    /// A new LeafNodeWriter initialized with the transaction's table root, version, timestamp,
    /// and root manifest URL. The root manifest URL is cached after the first lookup.
    pub fn new_leaf_node_writer(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<crate::transaction::leaf_writer::LeafNodeWriter> {
        // Get or fetch the root manifest URL (cached after first access)
        let root_manifest_url = if let Some(url) = self.cached_root_manifest_url.get() {
            url.clone()
        } else {
            let url = self.root_manifest_url(engine)?;
            let _ = self.cached_root_manifest_url.set(url.clone());
            url
        };

        let track_root_removals = !self.root_released;

        // Compute root manifest path once here and pass it through
        let root_manifest_path = root_manifest_url
            .as_ref()
            .map(|url| {
                crate::content_tree::absolute_to_relative_path(url, self.read_snapshot.table_root())
            })
            .transpose()?;

        // Get physical schema with PARQUET:field_id for adaptive metadata tree
        let column_mapping_mode = self
            .read_snapshot
            .table_configuration()
            .column_mapping_mode();
        let physical_schema = Arc::new(
            self.read_snapshot
                .schema()
                .as_ref()
                .make_physical(column_mapping_mode),
        );

        let writer = crate::transaction::leaf_writer::LeafNodeWriter::new(
            self.read_snapshot.table_root().clone(),
            self.read_snapshot.version() + 1,
            self.snapshot_id,
            physical_schema,
            track_root_removals,
            root_manifest_path,
        );

        Ok(writer)
    }

    /// Get the root manifest URL from the latest content root, if it exists.
    ///
    /// # Arguments
    /// * `engine` - The engine to use for reading the log segment
    ///
    /// # Returns
    /// * `Ok(Some(Url))` - The URL of the root manifest
    /// * `Ok(None)` - No content root exists yet
    /// * `Err` - Error reading the log segment
    pub fn root_manifest_url(&self, _engine: &dyn Engine) -> DeltaResult<Option<Url>> {
        let content_root = self.read_snapshot.content_root();
        let table_root = self.read_snapshot.table_root();
        Ok(content_root.and_then(|cr| table_root.join(&cr.path).ok()))
    }

    /// Incorporate leaf writer results into this transaction.
    ///
    /// This method:
    /// - Checks for duplicate unreconciled files across leaves (returns error if found)
    /// - Aggregates manifest deletion vectors (unions roaring bitmaps)
    /// - Collects leaf manifest entries to include in root
    ///
    /// # Arguments
    /// * `leaf_result` - The result from calling finish() on a LeafNodeWriter
    ///
    /// # Returns
    /// Ok(()) on success.
    pub fn add_leaf(
        &mut self,
        leaf_result: crate::transaction::leaf_writer::LeafNodeWriterResult,
    ) -> DeltaResult<()> {
        // Aggregate root entries to remove
        // These are file paths from the root manifest that have been moved to leaf manifests
        for file_path in leaf_result.root_entries_to_remove {
            // Track files that have been moved to leaves to prevent re-adding to root
            // Use empty string for DVUniqueId since these are data files
            self.aggregated_unreconciled
                .insert((file_path, String::new()));
        }

        // Aggregate root DV entries to remove
        for dv_path in leaf_result.root_dv_entries_to_remove {
            self.aggregated_root_dv_actions.insert(dv_path);
        }

        // Aggregate manifest DVs (union roaring bitmaps)
        for (manifest_url, row_indices) in leaf_result.manifest_dvs {
            let entry = self
                .aggregated_manifest_dvs
                .entry(manifest_url)
                .or_default();
            *entry |= row_indices;
        }

        // Collect leaf manifests
        if let Some(data_manifest) = leaf_result.data_file_manifest_written {
            self.leaf_manifests.push(data_manifest);
        }
        Ok(())
    }

    /// Same as [`Transaction::with_data_change`] but set the value directly instead of
    /// using a fluent API.
    #[internal_api]
    #[allow(dead_code)] // used in FFI
    pub(crate) fn set_data_change(&mut self, data_change: bool) {
        self.data_change = data_change;
    }

    /// Set the engine info field of this transaction's commit info action. This field is optional.
    pub fn with_engine_info(mut self, engine_info: impl Into<String>) -> Self {
        self.engine_info = Some(engine_info.into());
        self
    }

    /// Include a SetTransaction (app_id and version) action for this transaction (with an optional
    /// `last_updated` timestamp).
    /// Note that each app_id can only appear once per transaction. That is, multiple app_ids with
    /// different versions are disallowed in a single transaction. If a duplicate app_id is
    /// included, the `commit` will fail (that is, we don't eagerly check app_id validity here).
    pub fn with_transaction_id(mut self, app_id: String, version: i64) -> Self {
        let set_transaction = SetTransaction::new(app_id, version, Some(self.commit_timestamp));
        self.set_transactions.push(set_transaction);
        self
    }

    /// Set domain metadata to be written to the Delta log.
    /// Note that each domain can only appear once per transaction. That is, multiple configurations
    /// of the same domain are disallowed in a single transaction, as well as setting and removing
    /// the same domain in a single transaction. If a duplicate domain is included, the commit will
    /// fail (that is, we don't eagerly check domain validity here).
    /// Setting metadata for multiple distinct domains is allowed.
    pub fn with_domain_metadata(mut self, domain: String, configuration: String) -> Self {
        self.user_domain_metadata_additions
            .push(DomainMetadata::new(domain, configuration));
        self
    }

    /// Validate that the transaction is eligible to be marked as a blind append.
    ///
    /// Note: Domain metadata additions/removals are allowed; blind append only constrains
    /// data-file operations and read predicates. Conflict resolution determines whether
    /// metadata changes are problematic.
    fn validate_blind_append_semantics(&self) -> DeltaResult<()> {
        if !self.is_blind_append {
            return Ok(());
        }
        require!(
            !self.is_create_table(),
            Error::invalid_transaction_state(
                "Blind append is not supported for create-table transactions",
            )
        );
        require!(
            !self.add_files_metadata.is_empty(),
            Error::invalid_transaction_state("Blind append requires at least one added data file")
        );
        require!(
            self.data_change,
            Error::invalid_transaction_state("Blind append requires data_change to be true")
        );
        require!(
            self.remove_files_metadata.is_empty(),
            Error::invalid_transaction_state("Blind append cannot remove files")
        );
        require!(
            self.dv_matched_files.is_empty(),
            Error::invalid_transaction_state("Blind append cannot update deletion vectors")
        );

        Ok(())
    }

    /// Returns true if this is a create-table transaction.
    /// A create-table transaction has operation "CREATE TABLE" and a pre-commit snapshot
    /// with PRE_COMMIT_VERSION.
    fn is_create_table(&self) -> bool {
        let is_create = self.operation.as_deref() == Some("CREATE TABLE");
        debug_assert!(
            !is_create || self.read_snapshot.version() == PRE_COMMIT_VERSION,
            "CREATE TABLE transaction must have PRE_COMMIT_VERSION snapshot"
        );
        is_create
    }

    /// Computes the in-commit timestamp for this transaction if ICT is enabled.
    /// Returns `None` if ICT is not enabled on the table.
    fn get_in_commit_timestamp(&self, engine: &dyn Engine) -> DeltaResult<Option<i64>> {
        let has_ict = self
            .read_snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::InCommitTimestamp);

        if has_ict && !self.is_create_table() {
            Ok(self
                .read_snapshot
                .get_in_commit_timestamp(engine)?
                .map(|prev_ict| {
                    // The Delta protocol requires the timestamp to be "the larger of two values":
                    // - The time at which the writer attempted the commit (current_time)
                    // - One millisecond later than the previous commit's inCommitTimestamp (last_commit_timestamp + 1)
                    self.commit_timestamp.max(prev_ict + 1)
                }))
        } else if has_ict && self.is_create_table() {
            // ICT is enabled but this is a create-table transaction - not yet supported
            Err(Error::unsupported(
                "InCommitTimestamp is not yet supported for create table",
            ))
        } else {
            Ok(None)
        }
    }

    /// Returns the commit version for this transaction.
    /// For existing table transactions, this is snapshot.version() + 1.
    /// For create-table transactions (PRE_COMMIT_VERSION + 1 wraps to 0), this is 0.
    fn get_commit_version(&self) -> Version {
        // PRE_COMMIT_VERSION (u64::MAX) + 1 wraps to 0, which is the correct first version
        self.read_snapshot.version().wrapping_add(1)
    }

    /// Returns true if this commit will be handled as a batch commit (content tree update).
    /// When true, add/remove actions are recorded in the content tree rather than the delta log.
    fn is_batch_commit_active(&self) -> bool {
        let can_batch_commit = self.batch_commit
            && self
                .read_snapshot
                .table_configuration()
                .protocol()
                .has_writer_feature(&crate::table_features::TableFeature::MetadataTreeExperimental);
        let has_work_to_do = !self.add_files_metadata.is_empty()
            || !self.remove_files_metadata.is_empty()
            || !self.leaf_manifests.is_empty()
            || self
                .read_snapshot
                .content_root()
                .map_or(self.read_snapshot.version() > 0, |cr| {
                    cr.version < self.read_snapshot.version()
                });
        can_batch_commit && has_work_to_do
    }
    /// Validate domain metadata operations for both create-table and existing-table transactions.
    ///
    /// Enforces the following rules:
    /// - DomainMetadata feature must be supported if any domain operations are present
    /// - System domains (in system_domain_metadata_additions) must correspond to a known feature
    /// - User domains cannot use the delta.* prefix (system-reserved)
    /// - Domain removals are not allowed in create-table transactions
    /// - No duplicate domains within a single transaction (across both user and system)
    fn validate_domain_metadata_operations(&self) -> DeltaResult<()> {
        // Feature validation (applies to all transactions with domain operations)
        let has_domain_ops = !self.system_domain_metadata_additions.is_empty()
            || !self.user_domain_metadata_additions.is_empty()
            || !self.user_domain_removals.is_empty();

        // Early return if no domain operations to validate
        if !has_domain_ops {
            return Ok(());
        }

        if !self
            .read_snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::DomainMetadata)
        {
            return Err(Error::unsupported(
                "Domain metadata operations require writer version 7 and the 'domainMetadata' writer feature",
            ));
        }

        let is_create = self.is_create_table();
        let mut seen_domains = HashSet::with_capacity(
            self.system_domain_metadata_additions.len()
                + self.user_domain_metadata_additions.len()
                + self.user_domain_removals.len(),
        );

        // Validate SYSTEM domain additions (from transforms, e.g., clustering)
        // System domains are only populated during create-table
        for dm in &self.system_domain_metadata_additions {
            let domain = dm.domain();

            // Validate the system domain corresponds to a known feature
            self.validate_system_domain_feature(domain)?;

            // Check for duplicates
            if !seen_domains.insert(domain) {
                return Err(Error::generic(format!(
                    "Metadata for domain {} already specified in this transaction",
                    domain
                )));
            }
        }

        // Validate USER domain additions (via with_domain_metadata API)
        for dm in &self.user_domain_metadata_additions {
            let domain = dm.domain();

            // Users cannot add system domains via the public API
            if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
                return Err(Error::generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled",
                ));
            }

            // Check for duplicates (spans both system and user domains)
            if !seen_domains.insert(domain) {
                return Err(Error::generic(format!(
                    "Metadata for domain {} already specified in this transaction",
                    domain
                )));
            }
        }

        // No removals allowed for create-table.
        // Note: CreateTableTransaction does not expose with_domain_metadata_removed(),
        // so this is a defensive check. See #1768.
        if is_create && !self.user_domain_removals.is_empty() {
            return Err(Error::unsupported(
                "Domain metadata removals are not supported in create-table transactions",
            ));
        }

        // Validate domain removals (for non-create-table)
        for domain in &self.user_domain_removals {
            // Cannot remove system domains
            if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
                return Err(Error::generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled",
                ));
            }

            // Check for duplicates
            if !seen_domains.insert(domain.as_str()) {
                return Err(Error::generic(format!(
                    "Metadata for domain {} already specified in this transaction",
                    domain
                )));
            }
        }

        Ok(())
    }

    /// Validate that a system domain corresponds to a known feature and that the feature is supported.
    ///
    /// This prevents arbitrary `delta.*` domains from being added during table creation.
    /// Each known system domain must have its corresponding feature enabled in the protocol.
    fn validate_system_domain_feature(&self, domain: &str) -> DeltaResult<()> {
        let table_config = self.read_snapshot.table_configuration();

        // Map domain to its required feature
        let required_feature = match domain {
            ROW_TRACKING_DOMAIN_NAME => Some(TableFeature::RowTracking),
            // Will be changed to a constant in a follow up clustering create table feature PR
            "delta.clustering" => Some(TableFeature::ClusteredTable),
            _ => {
                return Err(Error::generic(format!(
                    "Unknown system domain '{}'. Only known system domains are allowed.",
                    domain
                )));
            }
        };

        // If the domain requires a feature, validate it's supported
        if let Some(feature) = required_feature {
            if !table_config.is_feature_supported(&feature) {
                return Err(Error::generic(format!(
                    "System domain '{}' requires the '{}' feature to be enabled",
                    domain, feature
                )));
            }
        }

        Ok(())
    }

    /// Generate removal actions for user domain metadata by scanning the log.
    ///
    /// This performs an expensive log replay operation to fetch the previous configuration
    /// value for each domain being removed, as required by the Delta spec for tombstones.
    /// Returns an empty vector if there are no domain removals.
    fn generate_user_domain_removal_actions(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Vec<DomainMetadata>> {
        if self.user_domain_removals.is_empty() {
            return Ok(vec![]);
        }

        // Scan log to fetch existing configurations for tombstones.
        // Pass the specific set of domains to remove so that log replay can terminate early
        // once all target domains have been found, instead of replaying the entire log.
        let domains: HashSet<&str> = self
            .user_domain_removals
            .iter()
            .map(String::as_str)
            .collect();
        let existing_domains = self
            .read_snapshot
            .log_segment()
            .scan_domain_metadatas(Some(&domains), engine)?;

        // Create removal tombstones with pre-image configurations
        Ok(self
            .user_domain_removals
            .iter()
            .filter_map(|domain| {
                // If domain doesn't exist in the log, this is a no-op (filter it out)
                existing_domains.get(domain).map(|existing| {
                    DomainMetadata::remove(domain.clone(), existing.configuration().to_owned())
                })
            })
            .collect())
    }

    /// Generate domain metadata actions with validation. Handle both user and system domains.
    ///
    /// This function may perform an expensive log replay operation if there are any domain removals.
    /// The log replay is required to fetch the previous configuration value for the domain to preserve
    /// in removal tombstones as mandated by the Delta spec.
    fn generate_domain_metadata_actions<'a>(
        &'a self,
        engine: &'a dyn Engine,
        row_tracking_high_watermark: Option<RowTrackingDomainMetadata>,
    ) -> DeltaResult<EngineDataResultIterator<'a>> {
        let is_create = self.is_create_table();

        // Validate domain operations (includes feature validation)
        self.validate_domain_metadata_operations()?;

        // TODO(sanuj) Create-table must not have row tracking or removals
        // Defensive. Needs to be updated when row tracking support is added.
        if is_create {
            if row_tracking_high_watermark.is_some() {
                return Err(Error::internal_error(
                    "CREATE TABLE cannot have row tracking domain metadata",
                ));
            }
            // user_domain_removals already validated above, but be explicit
            debug_assert!(self.user_domain_removals.is_empty());
        }

        // Generate removal actions (empty for create-table due to validation above)
        let removal_actions = self.generate_user_domain_removal_actions(engine)?;

        // Generate row tracking domain action (None for create-table)
        let row_tracking_domain_action = row_tracking_high_watermark
            .map(DomainMetadata::try_from)
            .transpose()?
            .into_iter();

        // Chain all domain actions and convert to EngineData
        // System domains first, then row tracking, then user domains, then removals
        Ok(Box::new(
            self.system_domain_metadata_additions
                .clone()
                .into_iter()
                .chain(row_tracking_domain_action)
                .chain(self.user_domain_metadata_additions.clone())
                .chain(removal_actions)
                .map(|dm| dm.into_engine_data(get_log_domain_metadata_schema().clone(), engine)),
        ))
    }

    /// The schema that the [`Engine`]'s [`ParquetHandler`] is expected to use when reporting information about
    /// a Parquet write operation back to Kernel.
    ///
    /// Concretely, it is the expected schema for [`EngineData`] passed to [`add_files`], as it is the base
    /// for constructing an add_file. Each row represents metadata about a
    /// file to be added to the table. Kernel takes this information and extends it to the full add_file
    /// action schema, adding internal fields (e.g., baseRowID) as necessary.
    ///
    /// The `stats` field contains file-level statistics. The schema returned here shows the base
    /// structure; the actual stats written by `DefaultEngine::write_parquet` include dynamically
    /// computed fields (numRecords, nullCount, minValues, maxValues, tightBounds) based on the
    /// data schema and table configuration. See [`stats_schema`] for the table-specific expected
    /// stats schema.
    ///
    /// Note: While currently static, in the future the schema might change depending on
    /// options set on the transaction or features enabled on the table.
    ///
    /// [`add_files`]: crate::transaction::Transaction::add_files
    /// [`ParquetHandler`]: crate::ParquetHandler
    /// [`stats_schema`]: Transaction::stats_schema
    pub fn add_files_schema(&self) -> &'static SchemaRef {
        &BASE_ADD_FILES_SCHEMA
    }

    /// Returns the expected schema for file statistics.
    ///
    /// The schema structure is derived from table configuration:
    /// - `delta.dataSkippingStatsColumns`: Explicit column list (if set)
    /// - `delta.dataSkippingNumIndexedCols`: Column count limit (default 32)
    /// - Partition columns: Always excluded
    ///
    /// The returned schema has the following structure:
    /// ```ignore
    /// {
    ///   numRecords: long,
    ///   nullCount: { ... },   // Nested struct mirroring data schema, all fields LONG
    ///   minValues: { ... },   // Nested struct, only min/max eligible types
    ///   maxValues: { ... },   // Nested struct, only min/max eligible types
    ///   tightBounds: boolean,
    /// }
    /// ```
    ///
    /// Engines should collect statistics matching this schema structure when writing files.
    ///
    /// Per the Delta protocol, clustering columns are always included in statistics,
    /// regardless of `dataSkippingStatsColumns` or `dataSkippingNumIndexedCols` settings.
    #[allow(unused)]
    pub fn stats_schema(&self) -> DeltaResult<SchemaRef> {
        let stats_schemas = self
            .read_snapshot
            .table_configuration()
            .build_expected_stats_schemas(self.clustering_columns.as_deref())?;
        Ok(stats_schemas.physical)
    }

    /// Returns the list of column names that should have statistics collected.
    ///
    /// This returns leaf column paths as [`ColumnName`] objects. Each `ColumnName`
    /// stores path components separately (e.g., `ColumnName::new(["nested", "field"])`).
    /// See [`ColumnName`'s `Display` implementation][ColumnName#impl-Display-for-ColumnName]
    /// for details on string formatting and escaping.
    ///
    /// Engines can use this to determine which columns need stats during writes.
    ///
    /// Per the Delta protocol, clustering columns are always included in statistics,
    /// regardless of `dataSkippingStatsColumns` or `dataSkippingNumIndexedCols` settings.
    #[allow(unused)]
    pub fn stats_columns(&self) -> Vec<ColumnName> {
        self.read_snapshot
            .table_configuration()
            .stats_column_names_physical(self.clustering_columns.as_deref())
    }

    // Generate the logical-to-physical transform expression which must be evaluated on every data
    // chunk before writing. At the moment, this is a transaction-wide expression.
    fn generate_logical_to_physical(&self) -> Expression {
        let partition_cols = self
            .read_snapshot
            .table_configuration()
            .partition_columns()
            .to_vec();
        // Check if materializePartitionColumns feature is enabled
        let materialize_partition_columns = self
            .read_snapshot
            .table_configuration()
            .is_feature_enabled(&TableFeature::MaterializePartitionColumns);
        // Build a Transform expression that drops partition columns from the input
        // (unless materializePartitionColumns is enabled).
        let mut transform = Transform::new_top_level();
        if !materialize_partition_columns {
            for col in &partition_cols {
                transform = transform.with_dropped_field_if_exists(col);
            }
        }
        Expression::transform(transform)
    }

    /// Get the write context for this transaction. At the moment, this is constant for the whole
    /// transaction.
    // Note: after we introduce metadata updates (modify table schema, etc.), we need to make sure
    // that engines cannot call this method after a metadata change, since the write context could
    // have invalid metadata.
    // Note: Callers that use get_write_context may be writing data to the table and they might
    // have invalid metadata.
    pub fn get_write_context(&self) -> WriteContext {
        let target_dir = self.read_snapshot.table_root();
        let snapshot_schema = self.read_snapshot.schema();
        let logical_to_physical = self.generate_logical_to_physical();
        let column_mapping_mode = self
            .read_snapshot
            .table_configuration()
            .column_mapping_mode();

        // Compute physical schema: exclude partition columns since they're stored in the path
        // (unless materializePartitionColumns is enabled), and apply column mapping to transform
        // logical field names to physical names.
        let partition_columns: Vec<String> = self
            .read_snapshot
            .table_configuration()
            .partition_columns()
            .to_vec();
        let materialize_partition_columns = self
            .read_snapshot
            .table_configuration()
            .is_feature_enabled(&TableFeature::MaterializePartitionColumns);
        let physical_fields = snapshot_schema
            .fields()
            .filter(|f| {
                materialize_partition_columns || !partition_columns.contains(&f.name().to_string())
            })
            .map(|f| f.make_physical(column_mapping_mode));
        let physical_schema = Arc::new(StructType::new_unchecked(physical_fields));

        // Get stats columns from table configuration
        let stats_columns = self.stats_columns();

        WriteContext::new(
            target_dir.clone(),
            snapshot_schema,
            physical_schema,
            Arc::new(logical_to_physical),
            column_mapping_mode,
            stats_columns,
        )
    }

    /// Add files to include in this transaction. This API generally enables the engine to
    /// add/append/insert data (files) to the table. Note that this API can be called multiple times
    /// to add multiple batches.
    ///
    /// The expected schema for `add_metadata` is given by [`Transaction::add_files_schema`].
    pub fn add_files(&mut self, add_metadata: Box<dyn EngineData>) {
        self.add_files_metadata.push(add_metadata);
    }

    /// Generate add actions, handling row tracking internally if needed
    #[instrument(name = "txn.gen_adds", skip_all, err)]
    fn generate_adds<'a>(
        &'a self,
        engine: &dyn Engine,
        commit_version: u64,
    ) -> DeltaResult<(
        EngineDataResultIterator<'a>,
        Option<RowTrackingDomainMetadata>,
    )> {
        fn build_add_actions<'a, I, T>(
            engine: &dyn Engine,
            add_files_metadata: I,
            input_schema: SchemaRef,
            output_schema: SchemaRef,
            data_change: bool,
        ) -> impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + 'a
        where
            I: Iterator<Item = DeltaResult<T>> + Send + 'a,
            T: Deref<Target = dyn EngineData> + Send + 'a,
        {
            let evaluation_handler = engine.evaluation_handler();

            add_files_metadata.map(move |add_files_batch| {
                // Convert stats to a JSON string and nest the add action in a top-level struct
                let transform = Expression::transform(
                    Transform::new_top_level()
                        .with_inserted_field(
                            Some("modificationTime"),
                            Expression::literal(data_change).into(),
                        )
                        .with_replaced_field(
                            "stats",
                            Expression::unary(ToJson, Expression::column(["stats"])).into(),
                        ),
                );
                let adds_expr = Expression::struct_from([transform]);
                let adds_evaluator = evaluation_handler.new_expression_evaluator(
                    input_schema.clone(),
                    Arc::new(adds_expr),
                    as_log_add_schema(output_schema.clone()).into(),
                )?;
                adds_evaluator.evaluate(add_files_batch?.deref())
            })
        }

        if self.add_files_metadata.is_empty() {
            return Ok((Box::new(iter::empty()), None));
        }

        let commit_version = i64::try_from(commit_version)
            .map_err(|_| Error::generic("Commit version too large to fit in i64"))?;

        let needs_row_tracking = self
            .read_snapshot
            .table_configuration()
            .should_write_row_tracking();

        // Row tracking is not yet supported for create-table with data
        if needs_row_tracking && self.is_create_table() {
            return Err(Error::unsupported(
                "Row tracking is not yet supported for create table with data",
            ));
        }

        if needs_row_tracking {
            // Read the current rowIdHighWaterMark from the snapshot's row tracking domain metadata
            let row_id_high_water_mark =
                RowTrackingDomainMetadata::get_high_water_mark(&self.read_snapshot, engine)?;

            // Create a row tracking visitor and visit all files to collect row tracking information
            let mut row_tracking_visitor = RowTrackingVisitor::new(
                row_id_high_water_mark,
                Some(self.add_files_metadata.len()),
            );

            // We visit all files with the row visitor before creating the add action iterator
            // because we need to know the final row ID high water mark to create the domain metadata action
            for add_files_batch in &self.add_files_metadata {
                row_tracking_visitor.visit_rows_of(add_files_batch.deref())?;
            }

            // Deconstruct the row tracking visitor to avoid borrowing issues
            let RowTrackingVisitor {
                base_row_id_batches,
                row_id_high_water_mark,
            } = row_tracking_visitor;

            // Create extended add files with row tracking columns
            let extended_add_files = self.add_files_metadata.iter().zip(base_row_id_batches).map(
                move |(add_files_batch, base_row_ids)| {
                    let commit_versions = vec![commit_version; base_row_ids.len()];
                    let base_row_ids_array =
                        ArrayData::try_new(ArrayType::new(DataType::LONG, true), base_row_ids)?;
                    let commit_versions_array =
                        ArrayData::try_new(ArrayType::new(DataType::LONG, true), commit_versions)?;

                    add_files_batch.append_columns(
                        with_row_tracking_cols(&Arc::new(StructType::new_unchecked(vec![]))),
                        vec![base_row_ids_array, commit_versions_array],
                    )
                },
            );

            // Generate add actions including row tracking metadata
            let add_actions = build_add_actions(
                engine,
                extended_add_files,
                with_row_tracking_cols(self.add_files_schema()),
                with_row_tracking_cols(&with_stats_col(&with_data_change_col(
                    self.add_files_schema(),
                ))),
                self.data_change,
            );

            // Generate a row tracking domain metadata based on the final high water mark
            let row_tracking_domain_metadata: RowTrackingDomainMetadata =
                RowTrackingDomainMetadata::new(row_id_high_water_mark);

            Ok((Box::new(add_actions), Some(row_tracking_domain_metadata)))
        } else {
            // Simple case without row tracking
            let add_actions = build_add_actions(
                engine,
                self.add_files_metadata.iter().map(|a| Ok(a.deref())),
                self.add_files_schema().clone(),
                with_stats_col(&with_data_change_col(self.add_files_schema())),
                self.data_change,
            );

            Ok((Box::new(add_actions), None))
        }
    }

    fn into_committed(self, file_meta: FileMeta) -> DeltaResult<CommittedTransaction> {
        let parsed_commit = ParsedLogPath::parse_commit(file_meta)?;

        let commit_version = parsed_commit.version;

        let post_commit_stats = PostCommitStats {
            commits_since_checkpoint: self.read_snapshot.log_segment().commits_since_checkpoint()
                + 1,
            commits_since_log_compaction: self
                .read_snapshot
                .log_segment()
                .commits_since_log_compaction_or_checkpoint()
                + 1,
        };

        Ok(CommittedTransaction {
            commit_version,
            post_commit_stats,
            post_commit_snapshot: Some(Arc::new(
                self.read_snapshot.new_post_commit(parsed_commit)?,
            )),
        })
    }

    fn into_conflicted(self, conflict_version: Version) -> ConflictedTransaction<S> {
        ConflictedTransaction {
            transaction: self,
            conflict_version,
        }
    }

    fn into_retryable(self, error: Error) -> RetryableTransaction<S> {
        RetryableTransaction {
            transaction: self,
            error,
        }
    }

    /// Generates Remove actions from scan file metadata.
    ///
    /// This internal method transforms scan row metadata into Remove actions for the Delta log.
    /// It's called during commit to process files staged via [`remove_files`] or files being
    /// updated with new deletion vectors via [`update_deletion_vectors`].
    ///
    /// # Parameters
    ///
    /// - `engine`: The engine used for expression evaluation
    /// - `remove_files_metadata`: Iterator over scan file metadata to transform into Remove actions
    /// - `columns_to_drop`: Column names to drop from the scan metadata before transformation.
    ///   This is used to remove temporary columns like the intermediate deletion vector column
    ///   added during DV updates.
    ///
    /// # Returns
    ///
    /// An iterator of FilteredEngineData containing Remove actions in the log schema format.
    ///
    /// [`remove_files`]: Transaction::remove_files
    /// [`update_deletion_vectors`]: Transaction::update_deletion_vectors
    #[instrument(name = "txn.gen_removes", skip_all, err)]
    fn generate_remove_actions<'a>(
        &'a self,
        engine: &dyn Engine,
        remove_files_metadata: impl Iterator<Item = &'a FilteredEngineData> + Send + 'a,
        columns_to_drop: &'a [&str],
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'a> {
        // Create-table transactions should not have any remove actions.
        // Only error if there are actually files queued for removal.
        if self.is_create_table() && !self.remove_files_metadata.is_empty() {
            return Err(Error::internal_error(
                "CREATE TABLE transaction cannot have remove actions",
            ));
        }

        let input_schema = scan_row_schema();
        let target_schema = NullableStatsTransform
            .transform_struct(get_log_remove_schema())
            .ok_or_else(|| Error::generic("Failed to transform remove schema"))?
            .into_owned();
        let evaluation_handler = engine.evaluation_handler();

        // Create the transform expression once, since it only contains literals and column references
        let mut transform = Transform::new_top_level()
            // deletionTimestamp
            .with_inserted_field(
                Some("path"),
                Expression::literal(self.commit_timestamp).into(),
            )
            // dataChange
            .with_inserted_field(Some("path"), Expression::literal(self.data_change).into())
            .with_inserted_field(
                // extended_file_metadata
                Some("path"),
                Expression::literal(true).into(),
            )
            .with_inserted_field(
                Some("path"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, "partitionValues"]).into(),
            )
            // tags
            .with_inserted_field(
                Some("stats"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, TAGS_NAME]).into(),
            )
            .with_inserted_field(
                Some("deletionVector"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, BASE_ROW_ID_NAME]).into(),
            )
            .with_inserted_field(
                Some("deletionVector"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME])
                    .into(),
            )
            // Preserve manifest location fields before dropping FILE_CONSTANT_VALUES_NAME
            // These fields tell the transaction whether files are in leaf manifests,
            // which determines whether to use delete_from_leaf vs mark_deleted
            .with_inserted_field(
                Some("deletionVector"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, "dataManifestPath"]).into(),
            )
            .with_inserted_field(
                Some("deletionVector"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, "dataManifestPosition"]).into(),
            )
            .with_dropped_field(FILE_CONSTANT_VALUES_NAME)
            .with_dropped_field("modificationTime")
            .with_dropped_field("numRecords");

        // Drop any additional columns specified in columns_to_drop
        for column_to_drop in columns_to_drop {
            transform = transform.with_dropped_field(*column_to_drop);
        }

        let expr = Arc::new(Expression::struct_from([Expression::transform(transform)]));

        let file_action_eval = Arc::new(evaluation_handler.new_expression_evaluator(
            input_schema.clone(),
            expr.clone(),
            target_schema.clone().into(),
        )?);

        Ok(remove_files_metadata.map(move |file_metadata_batch| {
            let updated_engine_data = file_action_eval.evaluate(file_metadata_batch.data())?;
            FilteredEngineData::try_new(
                updated_engine_data,
                file_metadata_batch.selection_vector().to_vec(),
            )
        }))
    }
}

/// WriteContext is data derived from a [`Transaction`] that can be provided to writers in order to
/// write table data.
///
/// [`Transaction`]: struct.Transaction.html
pub struct WriteContext {
    target_dir: Url,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    logical_to_physical: ExpressionRef,
    column_mapping_mode: ColumnMappingMode,
    /// Column names that should have statistics collected during writes.
    stats_columns: Vec<ColumnName>,
}

impl WriteContext {
    fn new(
        target_dir: Url,
        logical_schema: SchemaRef,
        physical_schema: SchemaRef,
        logical_to_physical: ExpressionRef,
        column_mapping_mode: ColumnMappingMode,
        stats_columns: Vec<ColumnName>,
    ) -> Self {
        WriteContext {
            target_dir,
            logical_schema,
            physical_schema,
            logical_to_physical,
            column_mapping_mode,
            stats_columns,
        }
    }

    pub fn target_dir(&self) -> &Url {
        &self.target_dir
    }

    pub fn logical_schema(&self) -> &SchemaRef {
        &self.logical_schema
    }

    pub fn physical_schema(&self) -> &SchemaRef {
        &self.physical_schema
    }

    pub fn logical_to_physical(&self) -> ExpressionRef {
        self.logical_to_physical.clone()
    }

    /// The [`ColumnMappingMode`] for this table.
    pub fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.column_mapping_mode
    }

    /// Returns the column names that should have statistics collected during writes.
    ///
    /// Based on table configuration (dataSkippingNumIndexedCols, dataSkippingStatsColumns).
    pub fn stats_columns(&self) -> &[ColumnName] {
        &self.stats_columns
    }

    /// Generate a new unique absolute URL for a deletion vector file.
    ///
    /// This method generates a unique file name in the table directory.
    /// Each call to this method returns a new unique path.
    ///
    /// # Arguments
    ///
    /// * `random_prefix` - A random prefix to use for the deletion vector file name.
    ///   Making this non-empty can help distributed load on object storage when writing/reading
    ///   to avoid throttling.  Typically a random string fo 2-4 characters is sufficient
    ///   for this purpose.
    ///
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let write_context = transaction.get_write_context();
    /// let dv_path = write_context.new_deletion_vector_path(String::from(rand_string()));
    /// // dv_url might be: s3://bucket/table/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin
    /// ```
    pub fn new_deletion_vector_path(&self, random_prefix: String) -> DeletionVectorPath {
        DeletionVectorPath::new(self.target_dir.clone(), random_prefix)
    }
}

/// Kernel exposes information about the state of the table that engines might want to use to
/// trigger actions like checkpointing or log compaction. This struct holds that information.
#[derive(Debug)]
pub struct PostCommitStats {
    /// The number of commits since this table has been checkpointed. Note that commit 0 is
    /// considered a checkpoint for the purposes of this computation.
    pub commits_since_checkpoint: u64,
    /// The number of commits since the log has been compacted on this table. Note that a checkpoint
    /// is considered a compaction for the purposes of this computation. Thus this is really the
    /// number of commits since a compaction OR a checkpoint.
    pub commits_since_log_compaction: u64,
}

/// The result of attempting to commit this transaction. If the commit was
/// successful/conflicted/retryable, the result is Ok(CommitResult), otherwise, if a nonrecoverable
/// error occurred, the result is Err(Error).
///
/// The commit result can be one of the following:
/// - [CommittedTransaction]: the transaction was successfully committed. [PostCommitStats] and
///   in the future a post-commit snapshot can be obtained from the committed transaction.
/// - [ConflictedTransaction]: the transaction conflicted with an existing version. This transcation
///   must be rebased before retrying. (currently no rebase APIs exist, caller must create new txn)
/// - [RetryableTransaction]: an IO (retryable) error occurred during the commit. This transaction
///   can be retried without rebasing.
#[derive(Debug)]
#[must_use]
pub enum CommitResult<S = ExistingTable> {
    /// The transaction was successfully committed.
    CommittedTransaction(CommittedTransaction),
    /// This transaction conflicted with an existing version (see
    /// [ConflictedTransaction::conflict_version]). The transaction
    /// is returned so the caller can resolve the conflict (along with the version which
    /// conflicted).
    // TODO(zach): in order to make the returning of a transaction useful, we need to add APIs to
    // update the transaction to a new version etc.
    ConflictedTransaction(ConflictedTransaction<S>),
    /// An IO (retryable) error occurred during the commit.
    RetryableTransaction(RetryableTransaction<S>),
}

impl<S> CommitResult<S> {
    /// Returns true if the commit was successful.
    pub fn is_committed(&self) -> bool {
        matches!(self, CommitResult::CommittedTransaction(_))
    }
}

/// This is the result of a successfully committed [Transaction]. One can retrieve the
/// [post_commit_stats], [commit version], and optionally the [post-commit snapshot] from this struct.
///
/// [post_commit_stats]: Self::post_commit_stats
/// [commit version]: Self::commit_version
/// [post-commit snapshot]: Self::post_commit_snapshot
#[derive(Debug)]
pub struct CommittedTransaction {
    /// The version of the table that was just committed.
    commit_version: Version,
    /// The [`PostCommitStats`] for this transaction.
    post_commit_stats: PostCommitStats,
    /// The [`SnapshotRef`] of the table after this transaction was committed.
    ///
    /// This is optional to allow incremental development of new features (e.g., table creation,
    /// transaction retries) without blocking on implementing post-commit snapshot support.
    post_commit_snapshot: Option<SnapshotRef>,
}

impl CommittedTransaction {
    /// The version of the table that was just sucessfully committed
    pub fn commit_version(&self) -> Version {
        self.commit_version
    }

    /// The [`PostCommitStats`] for this transaction
    pub fn post_commit_stats(&self) -> &PostCommitStats {
        &self.post_commit_stats
    }

    /// The [`SnapshotRef`] of the table after this transaction was committed.
    pub fn post_commit_snapshot(&self) -> Option<&SnapshotRef> {
        self.post_commit_snapshot.as_ref()
    }
}

/// This is the result of a conflicted [Transaction]. One can retrieve the [conflict version] from
/// this struct. In the future a rebase API will be provided (issue #1389).
///
/// [conflict version]: Self::conflict_version
#[derive(Debug)]
pub struct ConflictedTransaction<S = ExistingTable> {
    // TODO: remove after rebase APIs
    #[allow(dead_code)]
    transaction: Transaction<S>,
    conflict_version: Version,
}

impl<S> ConflictedTransaction<S> {
    /// The version attempted commit that yielded a conflict
    pub fn conflict_version(&self) -> Version {
        self.conflict_version
    }
}

/// A transaction that failed to commit due to a retryable error (e.g. IO error). The transaction
/// can be recovered with `RetryableTransaction::transaction` and retried without rebasing. The
/// associated error can be inspected via `RetryableTransaction::error`.
#[derive(Debug)]
pub struct RetryableTransaction<S = ExistingTable> {
    /// The transaction that failed to commit due to a retryable error.
    pub transaction: Transaction<S>,
    /// Transient error that caused the commit to fail.
    pub error: Error,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::{
        ArrayRef, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
    };
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::{FileSystemCommitter, PublishMetadata};
    use crate::engine::arrow_conversion::TryIntoArrow;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowEvaluationHandler;
    use crate::engine::sync::SyncEngine;
    use crate::schema::MapType;
    use crate::schema::{ColumnMetadataKey, MetadataValue};
    use crate::table_features::ColumnMappingMode;
    use crate::transaction::create_table::create_table;
    use crate::utils::test_utils::{
        load_test_table, string_array_to_engine_data, test_schema_flat, test_schema_nested,
        test_schema_with_array, test_schema_with_map,
    };
    use crate::EvaluationHandler;
    use crate::Snapshot;
    use rstest::rstest;
    use std::path::PathBuf;

    /// Helper function to create a logical test table schema with column mapping metadata.
    /// This returns the logical schema (with delta.columnMapping.* metadata).
    /// Use test_table_physical_schema() when creating ContentTreeNodeBuilder instances.
    fn test_table_schema() -> StructType {
        StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, true).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-a7f4159c".to_string()),
                ),
            ]),
            StructField::new("value", DataType::STRING, true).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(2),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-5f422f40".to_string()),
                ),
            ]),
        ])
    }

    /// Returns the physical schema for test tables with column mapping enabled.
    /// Use this when creating ContentTreeNodeBuilder instances in tests.
    fn test_table_physical_schema() -> StructType {
        test_table_schema().make_physical(crate::table_features::ColumnMappingMode::Id)
    }

    /// A mock committer that always returns an IOError, used to test the retryable error path.
    struct IoErrorCommitter;

    impl Committer for IoErrorCommitter {
        fn commit(
            &self,
            _engine: &dyn Engine,
            _actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
            _commit_metadata: CommitMetadata,
        ) -> DeltaResult<CommitResponse> {
            Err(Error::IOError(std::io::Error::other("simulated IO error")))
        }
        fn is_catalog_committer(&self) -> bool {
            false
        }
        fn publish(
            &self,
            _engine: &dyn Engine,
            _publish_metadata: PublishMetadata,
        ) -> DeltaResult<()> {
            Ok(())
        }
    }

    /// Sets up a snapshot for a table with deletion vector support at version 1
    fn setup_dv_enabled_table() -> (SyncEngine, Arc<Snapshot>) {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url)
            .at_version(1)
            .build(&engine)
            .unwrap();
        (engine, snapshot)
    }

    fn setup_non_dv_table() -> (SyncEngine, Arc<Snapshot>) {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        (engine, snapshot)
    }

    /// Creates a test deletion vector descriptor with default values (the DV might not exist on disk)
    fn create_test_dv_descriptor(path_suffix: &str) -> DeletionVectorDescriptor {
        use crate::actions::deletion_vector::{
            DeletionVectorDescriptor, DeletionVectorStorageType,
        };
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: format!("dv_{}", path_suffix),
            offset: Some(0),
            size_in_bytes: 100,
            cardinality: 1,
        }
    }

    fn create_dv_transaction(
        snapshot: Arc<Snapshot>,
        engine: &dyn Engine,
    ) -> DeltaResult<Transaction> {
        Ok(snapshot
            .transaction(Box::new(FileSystemCommitter::new()), engine)?
            .with_operation("DELETE".to_string())
            .with_engine_info("test_engine"))
    }

    // TODO: create a finer-grained unit tests for transactions (issue#1091)
    #[test]
    fn test_add_files_schema() -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url)
            .at_version(1)
            .build(&engine)
            .unwrap();
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?
            .with_engine_info("default engine");

        let schema = txn.add_files_schema();
        let expected = StructType::new_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null(
                "partitionValues",
                MapType::new(DataType::STRING, DataType::STRING, true),
            ),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("modificationTime", DataType::LONG),
            StructField::nullable(
                "stats",
                DataType::struct_type_unchecked(vec![
                    StructField::nullable("numRecords", DataType::LONG),
                    StructField::nullable("nullCount", DataType::struct_type_unchecked(vec![])),
                    StructField::nullable("minValues", DataType::struct_type_unchecked(vec![])),
                    StructField::nullable("maxValues", DataType::struct_type_unchecked(vec![])),
                    StructField::nullable("tightBounds", DataType::BOOLEAN),
                ]),
            ),
        ]);
        assert_eq!(*schema, expected.into());
        Ok(())
    }

    #[test]
    fn test_with_batch_commit() -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url)
            .at_version(1)
            .build(&engine)
            .unwrap();

        // Test that with_batch_commit returns self correctly
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?
            .with_batch_commit()
            .with_engine_info("test engine");

        // Verify batch_commit flag is set
        assert!(txn.batch_commit);
        Ok(())
    }

    #[test]
    fn test_batch_commit_default_false() -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url)
            .at_version(1)
            .build(&engine)
            .unwrap();

        // Test that batch_commit defaults to false
        let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
        assert!(!txn.batch_commit);
        Ok(())
    }

    #[test]
    fn test_new_deletion_vector_path() -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url.clone())
            .at_version(1)
            .build(&engine)
            .unwrap();
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?
            .with_engine_info("default engine");
        let write_context = txn.get_write_context();

        // Test with empty prefix
        let dv_path1 = write_context.new_deletion_vector_path(String::from(""));
        let abs_path1 = dv_path1.absolute_path()?;
        assert!(abs_path1.as_str().contains(url.as_str()));

        // Test with non-empty prefix
        let prefix = String::from("dv_test");
        let dv_path2 = write_context.new_deletion_vector_path(prefix.clone());
        let abs_path2 = dv_path2.absolute_path()?;
        assert!(abs_path2.as_str().contains(url.as_str()));
        assert!(abs_path2.as_str().contains(&prefix));

        // Test that two paths with same prefix are different (unique UUIDs)
        let dv_path3 = write_context.new_deletion_vector_path(prefix.clone());
        let abs_path3 = dv_path3.absolute_path()?;
        assert_ne!(abs_path2, abs_path3);

        Ok(())
    }

    #[test]
    fn test_physical_schema_excludes_partition_columns() -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?
            .with_engine_info("default engine");

        let write_context = txn.get_write_context();
        let logical_schema = write_context.logical_schema();
        let physical_schema = write_context.physical_schema();

        // Logical schema should include the partition column
        assert!(
            logical_schema.contains("letter"),
            "Logical schema should contain partition column 'letter'"
        );

        // Physical schema should exclude the partition column
        assert!(
            !physical_schema.contains("letter"),
            "Physical schema should not contain partition column 'letter' (stored in path)"
        );

        // Both should contain the non-partition columns
        assert!(
            logical_schema.contains("number"),
            "Logical schema should contain data column 'number'"
        );

        assert!(
            physical_schema.contains("number"),
            "Physical schema should contain data column 'number'"
        );

        Ok(())
    }

    /// Helper: loads a test table snapshot and returns both the snapshot and its write context.
    fn snapshot_and_write_context(
        table_path: &str,
    ) -> Result<(Arc<Snapshot>, WriteContext), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from(table_path)).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine)?;
        let txn = snapshot
            .clone()
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?;
        Ok((snapshot, txn.get_write_context()))
    }

    /// Helper: evaluates the logical-to-physical transform on the given batch and returns the
    /// output RecordBatch.
    fn eval_logical_to_physical(
        wc: &WriteContext,
        batch: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let logical_schema = wc.logical_schema();
        let physical_schema = wc.physical_schema();
        let l2p = wc.logical_to_physical();

        let handler = ArrowEvaluationHandler;
        let evaluator = handler.new_expression_evaluator(
            logical_schema.clone(),
            l2p,
            physical_schema.clone().into(),
        )?;
        let result = ArrowEngineData::try_from_engine_data(
            evaluator.evaluate(&ArrowEngineData::new(batch))?,
        )?;
        Ok(result.record_batch().clone())
    }

    #[test]
    fn test_materialize_partition_columns_in_write_context(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Without materializePartitionColumns, partition column should be dropped
        let (snap_without, wc_without) =
            snapshot_and_write_context("./tests/data/basic_partitioned/")?;
        let partition_cols = snap_without.table_configuration().partition_columns();
        assert_eq!(partition_cols.len(), 1);
        assert_eq!(partition_cols[0], "letter");
        assert!(
            !snap_without
                .table_configuration()
                .protocol()
                .has_table_feature(&TableFeature::MaterializePartitionColumns),
            "basic_partitioned should not have materializePartitionColumns feature"
        );
        let expr_str = format!("{}", wc_without.logical_to_physical());
        assert!(
            expr_str.contains("drop letter"),
            "Partition column 'letter' should be dropped. Expression: {}",
            expr_str
        );

        // With materializePartitionColumns, no columns should be dropped (identity transform)
        let (snap_with, wc_with) =
            snapshot_and_write_context("./tests/data/partitioned_with_materialize_feature/")?;
        let partition_cols = snap_with.table_configuration().partition_columns();
        assert_eq!(partition_cols.len(), 1);
        assert_eq!(partition_cols[0], "letter");
        assert!(
            snap_with
                .table_configuration()
                .protocol()
                .has_table_feature(&TableFeature::MaterializePartitionColumns),
            "partitioned_with_materialize_feature should have materializePartitionColumns feature"
        );
        let expr_str = format!("{}", wc_with.logical_to_physical());
        assert!(
            !expr_str.contains("drop"),
            "No columns should be dropped with materializePartitionColumns. Expression: {}",
            expr_str
        );

        Ok(())
    }

    /// Physical schema should include partition columns when materializePartitionColumns is on.
    #[test]
    fn test_physical_schema_includes_partition_columns_when_materialized(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/partitioned_with_materialize_feature/",
        ))
        .unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).at_version(1).build(&engine)?;

        let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
        let write_context = txn.get_write_context();
        let physical_schema = write_context.physical_schema();

        assert!(
            physical_schema.contains("letter"),
            "Partition column 'letter' should be in physical schema when materialized"
        );
        assert!(
            physical_schema.contains("number"),
            "Non-partition column 'number' should be in physical schema"
        );
        Ok(())
    }

    /// Tests that update_deletion_vectors validates table protocol requirements.
    /// Validates that attempting DV updates on unsupported tables returns protocol error.
    #[test]
    fn test_update_deletion_vectors_unsupported_table() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) = setup_non_dv_table();
        let mut txn = create_dv_transaction(snapshot, &engine)?;

        let dv_map = HashMap::new();
        let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

        let err = result.expect_err("Should fail on table without DV support");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Deletion vector")
                && (err_msg.contains("require") || err_msg.contains("version")),
            "Expected protocol error about DV requirements, got: {}",
            err_msg
        );
        Ok(())
    }

    /// Tests that update_deletion_vectors validates DV descriptors match scan files.
    /// Validates detection of mismatch between provided DV descriptors and actual files.
    #[test]
    fn test_update_deletion_vectors_mismatch_count() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) = setup_dv_enabled_table();
        let mut txn = create_dv_transaction(snapshot, &engine)?;

        let mut dv_map = HashMap::new();
        let descriptor = create_test_dv_descriptor("non_existent");
        dv_map.insert("non_existent_file.parquet".to_string(), descriptor);

        let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

        assert!(
            result.is_err(),
            "Should fail when DV descriptors don't match scan files"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("matched") && err_msg.contains("does not match"),
            "Expected error about mismatched count (expected 1 descriptor, 0 matched files), got: {}",
            err_msg);
        Ok(())
    }

    /// Tests that update_deletion_vectors handles empty DV updates correctly as a no-op.
    /// This edge case occurs when a DELETE operation matches no rows.
    #[test]
    fn test_update_deletion_vectors_empty_inputs() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) = setup_dv_enabled_table();
        let mut txn = create_dv_transaction(snapshot, &engine)?;

        let dv_map = HashMap::new();
        let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

        assert!(
            result.is_ok(),
            "Empty DV updates should succeed as no-op, got error: {:?}",
            result
        );

        Ok(())
    }

    // ============================================================================
    // validate_blind_append tests
    // ============================================================================
    fn add_dummy_file<S>(txn: &mut Transaction<S>) {
        let data = string_array_to_engine_data(StringArray::from(vec!["dummy"]));
        txn.add_files(data);
    }

    fn create_existing_table_txn(
    ) -> DeltaResult<(Arc<dyn Engine>, Transaction, Option<tempfile::TempDir>)> {
        let (engine, snapshot, tempdir) = load_test_table("table-without-dv-small")?;
        let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
        Ok((engine, txn, tempdir))
    }

    #[test]
    fn test_validate_blind_append_success() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        add_dummy_file(&mut txn);
        txn.validate_blind_append_semantics()?;
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_requires_adds() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        let result = txn.validate_blind_append_semantics();
        assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_requires_data_change() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        txn.set_data_change(false);
        add_dummy_file(&mut txn);
        let result = txn.validate_blind_append_semantics();
        assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_rejects_removes() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        add_dummy_file(&mut txn);
        let remove_data = FilteredEngineData::with_all_rows_selected(string_array_to_engine_data(
            StringArray::from(vec!["remove"]),
        ));
        txn.remove_files(remove_data);
        let result = txn.validate_blind_append_semantics();
        assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_rejects_dv_updates() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        add_dummy_file(&mut txn);
        let dv_data = FilteredEngineData::with_all_rows_selected(string_array_to_engine_data(
            StringArray::from(vec!["dv"]),
        ));
        txn.dv_matched_files.push(dv_data);
        let result = txn.validate_blind_append_semantics();
        assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_rejects_create_table() -> DeltaResult<()> {
        let tempdir = tempfile::tempdir()?;
        let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
            "id",
            DataType::INTEGER,
        )])?);
        let store = Arc::new(object_store::local::LocalFileSystem::new());
        let engine = Arc::new(crate::engine::default::DefaultEngineBuilder::new(store).build());
        let mut txn = create_table(
            tempdir.path().to_str().expect("valid temp path"),
            schema,
            "test_engine",
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        // CreateTableTransaction does not expose with_blind_append() (compile-time
        // prevention per #1768). Directly set the field to test the runtime check.
        txn.is_blind_append = true;
        add_dummy_file(&mut txn);
        let result = txn.validate_blind_append_semantics();
        assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
        Ok(())
    }

    #[test]
    fn test_blind_append_sets_commit_info_flag() -> Result<(), Box<dyn std::error::Error>> {
        let commit_info = CommitInfo::new(1, None, None, None, 0, true);
        assert_eq!(commit_info.is_blind_append, Some(true));

        let commit_info_false = CommitInfo::new(1, None, None, None, 0, false);
        assert_eq!(commit_info_false.is_blind_append, None);
        Ok(())
    }

    #[test]
    fn test_blind_append_commit_rejects_no_adds() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        // No files added — commit should fail with blind append validation
        let err = txn
            .commit(_engine.as_ref())
            .expect_err("Blind append with no adds should fail");
        assert!(
            err.to_string()
                .contains("Blind append requires at least one added data file"),
            "Unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn test_blind_append_commit_success() -> DeltaResult<()> {
        let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        add_dummy_file(&mut txn);
        // Blind append with add files should pass validation and proceed to commit.
        // The commit itself may fail due to schema mismatch with the dummy data,
        // but we verify validation (line 415) passes on the Ok path.
        let result = txn.commit(engine.as_ref());
        // If it fails, it should NOT be an InvalidTransactionState error
        if let Err(e) = result {
            assert!(
                !matches!(e, Error::InvalidTransactionState(_)),
                "Blind append validation should have passed, got: {e}"
            );
        }
        Ok(())
    }

    // Note: Additional test coverage for partial file matching (where some files in a scan
    // have DV updates but others don't) is provided by the end-to-end integration test
    // kernel/tests/dv.rs and kernel/tests/write.rs, which exercises
    // the full deletion vector write workflow including the DvMatchVisitor logic.

    /// Helper to create an initial Delta table with Protocol and Metadata (version 0)
    ///
    /// # Arguments
    /// * `table_root` - The table root URL
    /// * `enable_column_mapping` - If true, enables column mapping mode 'id' (required for batch_commit/content metadata trees)
    fn create_initial_table(table_root: &Url, enable_column_mapping: bool) -> DeltaResult<()> {
        use serde_json::json;
        use std::fs::{create_dir_all, write};
        use uuid::Uuid;

        let table_id = Uuid::new_v4().to_string();

        // Schema with or without column mapping metadata
        let schema = if enable_column_mapping {
            json!({
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "integer", "nullable": true, "metadata": {
                        "delta.columnMapping.id": 1,
                        "delta.columnMapping.physicalName": "col-a7f4159c"
                    }},
                    {"name": "value", "type": "string", "nullable": true, "metadata": {
                        "delta.columnMapping.id": 2,
                        "delta.columnMapping.physicalName": "col-5f422f40"
                    }}
                ]
            })
        } else {
            json!({
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "integer", "nullable": true, "metadata": {}},
                    {"name": "value", "type": "string", "nullable": true, "metadata": {}}
                ]
            })
        };

        let protocol = if enable_column_mapping {
            json!({
                "protocol": {
                    "minReaderVersion": 3,
                    "minWriterVersion": 7,
                    "readerFeatures": ["columnMapping", "metadataTree-experimental"],
                    "writerFeatures": ["columnMapping", "metadataTree-experimental"]
                }
            })
        } else {
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2,
                    "readerFeatures": [],
                    "writerFeatures": []
                }
            })
        };

        let configuration = if enable_column_mapping {
            json!({"delta.columnMapping.mode": "id"})
        } else {
            json!({})
        };

        let metadata = json!({
            "metaData": {
                "id": table_id,
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": schema.to_string(),
                "partitionColumns": [],
                "configuration": configuration,
                "createdTime": 1677811175819u64
            }
        });

        let data = [
            serde_json::to_vec(&protocol)?,
            b"\n".to_vec(),
            serde_json::to_vec(&metadata)?,
        ]
        .concat();

        let delta_log_path = table_root
            .join("_delta_log/")?
            .to_file_path()
            .map_err(|_| Error::generic("Cannot convert URL to file path"))?;

        create_dir_all(&delta_log_path)
            .map_err(|e| Error::generic(format!("Failed to create _delta_log: {e}")))?;

        let file_path = delta_log_path.join("00000000000000000000.json");
        write(&file_path, data)
            .map_err(|e| Error::generic(format!("Failed to write initial log: {e}")))?;

        Ok(())
    }

    /// Helper to write a ContentRoot action to a specific version
    fn write_content_root_action(
        table_root: &Url,
        content_root_path: &str,
        version: u64,
    ) -> DeltaResult<()> {
        use serde_json::json;
        use std::fs::write;

        let content_root = json!({
            "contentRoot": {
                "path": content_root_path,
                "sizeInBytes": 0,
                "version": version
            }
        });

        let data = serde_json::to_vec(&content_root)?;

        let delta_log_path = table_root
            .join("_delta_log/")?
            .to_file_path()
            .map_err(|_| Error::generic("Cannot convert URL to file path"))?;

        let file_name = format!("{:020}.json", version);
        let file_path = delta_log_path.join(file_name);
        write(&file_path, data)
            .map_err(|e| Error::generic(format!("Failed to write ContentRoot: {e}")))?;

        Ok(())
    }

    /// Helper to get file list from scan
    fn get_files_from_scan(table_root: &Url, engine: &dyn Engine) -> DeltaResult<Vec<String>> {
        let snapshot = crate::Snapshot::builder_for(table_root.clone()).build(engine)?;
        let scan = snapshot.scan_builder().build()?;
        // Use get_files_for_scan_allow_dvs since content root tests may have DVs populated
        crate::scan::tests::get_files_for_scan_allow_dvs(scan, engine)
    }

    /// Helper to create an Add action with minimal boilerplate
    fn make_add_action(path: String) -> crate::actions::Add {
        crate::actions::Add {
            path,
            partition_values: Default::default(),
            size: 1024,
            modification_time: 1677811178336,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            data_manifest_path: None,
            data_manifest_position: None,
        }
    }

    /// Tests that removing files with data_manifest_path uses delete_from_leaf properly
    /// by verifying through the Scan API that files are removed
    #[test]
    fn test_remove_with_data_in_leaf_manifest() -> Result<(), Box<dyn std::error::Error>> {
        use crate::committer::FileSystemCommitter;
        use crate::content_tree::builder::ContentTreeNodeBuilder;
        use crate::content_tree::writer::ContentTreeNodeWriter;
        use crate::engine::sync::SyncEngine;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        // Canonicalize the path to match what try_parse_uri does in real usage
        // This ensures paths are consistent (e.g., /private/var instead of /var on macOS)
        let canonical_path = std::fs::canonicalize(temp_dir.path())?;
        let table_root = Url::from_directory_path(canonical_path).unwrap();

        // Step 1: Create initial table (v0) with Protocol + Metadata
        // Enable column mapping for batch_commit mode (content metadata trees)
        create_initial_table(&table_root, true)?;

        // Step 2: Build metadata tree with leaf manifest containing Add actions
        let mut leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_physical_schema());
        let data_files: Vec<String> = (0..5).map(|i| format!("data/file-{}.parquet", i)).collect();

        for path in &data_files {
            leaf_builder.add(make_add_action(path.clone()), 1, 1)?;
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, 1)?;
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_physical_schema());
        root_builder.add_entry(leaf_manifest_entry);
        let root_metadata = root_builder.build(&engine, 1)?;
        let (root_url, _) = ContentTreeNodeWriter::try_new(root_metadata)?.write(&engine)?;

        // Step 3: Write ContentRoot action (v1)
        write_content_root_action(&table_root, root_url.as_str(), 1)?;

        // Step 4: Scan to get initial file list
        let initial_files: Vec<String> = get_files_from_scan(&table_root, &engine)?
            .into_iter()
            .filter(|f| !f.contains(".content.")) // Filter out ContentRoot metadata files
            .collect();
        assert_eq!(initial_files.len(), 5, "Expected 5 data files");

        // Step 5: Use Transaction API to remove file at index 2 (v2)
        let snapshot = crate::Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let scan = snapshot.clone().scan_builder().build()?;

        let committer = Box::new(FileSystemCommitter::new());
        let mut txn = snapshot
            .transaction(committer, &engine)?
            .with_batch_commit()
            .with_operation("DELETE".to_string());

        // Remove file at row index 2 within the scan batch
        // With batched EngineData creation, all files are now in a single batch
        let mut scan_metadata_iter = scan.scan_metadata(&engine)?;
        if let Some(res) = scan_metadata_iter.next() {
            let scan_data = res?;
            let num_rows = scan_data.scan_files.data().len();

            // Create a selection vector that selects only row 2
            let mut selection_vector = vec![false; num_rows];
            if num_rows > 2 {
                selection_vector[2] = true;

                // Extract the underlying data and create new filtered data with our selection
                let (data, _old_selection) = scan_data.scan_files.into_parts();
                let filtered_files = FilteredEngineData::try_new(data, selection_vector)?;

                txn.remove_files(filtered_files);
            }
        }

        // Commit the transaction
        let _committed = match txn.commit(&engine)? {
            CommitResult::CommittedTransaction(c) => c,
            _ => panic!("Transaction should succeed"),
        };

        // Step 6: Scan again to verify file was removed
        let final_files: Vec<String> = get_files_from_scan(&table_root, &engine)?
            .into_iter()
            .filter(|f| !f.contains(".content.")) // Filter out ContentRoot metadata files
            .collect();
        assert_eq!(final_files.len(), 4, "Expected 4 data files after removal");

        // Verify that one of the original files is now missing
        let removed_count = initial_files
            .iter()
            .filter(|f| !final_files.contains(f))
            .count();
        assert_eq!(removed_count, 1, "Expected exactly 1 file to be removed");

        Ok(())
    }

    /// Regression test: remove actions must NOT appear in the delta log when using batch commit.
    /// In batch commit mode, removes are recorded in the content tree (manifest DV), so writing
    /// them to the log as well would cause double-counting on replay.
    #[test]
    fn test_batch_commit_remove_not_written_to_log() -> Result<(), Box<dyn std::error::Error>> {
        use crate::committer::FileSystemCommitter;
        use crate::content_tree::builder::ContentTreeNodeBuilder;
        use crate::content_tree::writer::ContentTreeNodeWriter;
        use crate::engine::sync::SyncEngine;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let canonical_path = std::fs::canonicalize(temp_dir.path())?;
        let table_root = Url::from_directory_path(canonical_path).unwrap();

        // Step 1: Create initial table (v0)
        create_initial_table(&table_root, true)?;

        // Step 2: Build a leaf manifest with 5 data files and write a content root (v1)
        let mut leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_physical_schema());
        let data_files: Vec<String> = (0..5).map(|i| format!("data/file-{}.parquet", i)).collect();
        for path in &data_files {
            leaf_builder.add(make_add_action(path.clone()), 1, 1)?;
        }
        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, 1)?;
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_physical_schema());
        root_builder.add_entry(leaf_manifest_entry);
        let root_metadata = root_builder.build(&engine, 1)?;
        let (root_url, _) = ContentTreeNodeWriter::try_new(root_metadata)?.write(&engine)?;
        write_content_root_action(&table_root, root_url.as_str(), 1)?;

        // Step 3: Batch-commit a remove (v2)
        let snapshot = crate::Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let scan = snapshot.clone().scan_builder().build()?;

        let committer = Box::new(FileSystemCommitter::new());
        let mut txn = snapshot
            .transaction(committer, &engine)?
            .with_batch_commit()
            .with_operation("DELETE".to_string());

        let mut scan_metadata_iter = scan.scan_metadata(&engine)?;
        if let Some(res) = scan_metadata_iter.next() {
            let scan_data = res?;
            let num_rows = scan_data.scan_files.data().len();
            let mut selection_vector = vec![false; num_rows];
            if num_rows > 2 {
                selection_vector[2] = true;
                let (data, _) = scan_data.scan_files.into_parts();
                txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);
            }
        }

        match txn.commit(&engine)? {
            CommitResult::CommittedTransaction(_) => {}
            _ => panic!("Transaction should succeed"),
        };

        // Step 4: Read the committed log file (v2) and assert no "remove" action is present.
        // Each line in a Delta log file is a JSON object; a remove action has a top-level "remove" key.
        let delta_log_path = table_root
            .join("_delta_log/")?
            .to_file_path()
            .map_err(|_| Error::generic("Cannot convert URL to file path"))?;
        let v2_log = std::fs::read_to_string(delta_log_path.join("00000000000000000002.json"))?;

        for line in v2_log.lines() {
            let json: serde_json::Value = serde_json::from_str(line)?;
            assert!(
                json.get("remove").is_none(),
                "batch commit should not write remove actions to the delta log, but found: {line}"
            );
        }

        Ok(())
    }

    /// Helper to create an Add action with a deletion vector
    fn make_add_action_with_dv(path: String, dv_path: String) -> crate::actions::Add {
        use crate::actions::deletion_vector::{
            DeletionVectorDescriptor, DeletionVectorStorageType,
        };

        crate::actions::Add {
            path,
            partition_values: Default::default(),
            size: 1024,
            modification_time: 1677811178336,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: Some(DeletionVectorDescriptor {
                storage_type: DeletionVectorStorageType::PersistedRelative,
                path_or_inline_dv: dv_path,
                offset: Some(0),
                size_in_bytes: 100,
                cardinality: 5,
            }),
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            data_manifest_path: None,
            data_manifest_position: None,
        }
    }

    /// Tests that removing files with deletion vectors in leaf manifests works properly
    #[test]
    fn test_remove_file_with_dv_in_leaf_manifest() -> Result<(), Box<dyn std::error::Error>> {
        use crate::committer::FileSystemCommitter;
        use crate::content_tree::builder::ContentTreeNodeBuilder;
        use crate::content_tree::writer::ContentTreeNodeWriter;
        use crate::content_tree::DataContentType;
        use crate::engine::sync::SyncEngine;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        // Canonicalize the path to match what try_parse_uri does in real usage
        // This ensures paths are consistent (e.g., /private/var instead of /var on macOS)
        let canonical_path = std::fs::canonicalize(temp_dir.path())?;
        let table_root = Url::from_directory_path(canonical_path).unwrap();

        // Step 1: Create initial table (v0) with Protocol + Metadata
        // Enable column mapping for batch_commit mode (content metadata trees)
        create_initial_table(&table_root, true)?;

        // Step 2: Build metadata tree with TWO leaf manifests:
        // - Data leaf manifest with data file entries (some reference DVs)
        // - Delete leaf manifest with PositionDeletes entries (the actual DV files)

        // Create data leaf manifest with 5 data files
        let mut data_leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_physical_schema());

        // Files without DV
        data_leaf_builder.add(make_add_action("data/file-0.parquet".to_string()), 1, 1)?;
        data_leaf_builder.add(make_add_action("data/file-1.parquet".to_string()), 1, 1)?;

        // Files with DV (file-2 and file-3) - use the builder's add() method which extracts DV content
        data_leaf_builder.add(
            make_add_action_with_dv(
                "data/file-2.parquet".to_string(),
                "vBn[lx{q8@P<9BNH/isA".to_string(), // Valid 20-char encoded UUID
            ),
            1,
            1,
        )?;
        data_leaf_builder.add(
            make_add_action_with_dv(
                "data/file-3.parquet".to_string(),
                "^-aqEH.-t@S}K{vb[*k^".to_string(), // Another valid 20-char encoded UUID
            ),
            1,
            1,
        )?;

        // File without DV
        data_leaf_builder.add(make_add_action("data/file-4.parquet".to_string()), 1, 1)?;

        let data_leaf_entry = data_leaf_builder.write_leaf(&engine, 1)?;

        // In the new CombinedManifest model, DV info is inline on Data entries.
        // No separate delete leaf is needed — DVs are already embedded via builder's add().
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_physical_schema());
        root_builder.add_entry(data_leaf_entry);
        let root_metadata = root_builder.build(&engine, 1)?;
        let (root_url, _) = ContentTreeNodeWriter::try_new(root_metadata)?.write(&engine)?;

        // Step 3: Write ContentRoot action (v1)
        write_content_root_action(&table_root, root_url.as_str(), 1)?;

        // Step 4: Scan to get initial file list
        let initial_files: Vec<String> = get_files_from_scan(&table_root, &engine)?
            .into_iter()
            .filter(|f| !f.contains(".content.")) // Filter out ContentRoot metadata files
            .collect();
        assert_eq!(initial_files.len(), 5, "Expected 5 data files");

        // Step 5: Use Transaction API to remove file-2 (which has a deletion vector)
        let snapshot = crate::Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let scan = snapshot.clone().scan_builder().build()?;

        let committer = Box::new(FileSystemCommitter::new());
        let mut txn = snapshot
            .transaction(committer, &engine)?
            .with_batch_commit()
            .with_operation("DELETE".to_string());

        // Remove file at row index 2 (file-2.parquet which has a DV)
        // With batched EngineData creation, all files are now in a single batch
        let mut scan_metadata_iter = scan.scan_metadata(&engine)?;
        if let Some(res) = scan_metadata_iter.next() {
            let scan_data = res?;
            let num_rows = scan_data.scan_files.data().len();

            // Create a selection vector that selects only row 2
            let mut selection_vector = vec![false; num_rows];
            if num_rows > 2 {
                selection_vector[2] = true;

                // Extract the underlying data and create new filtered data with our selection
                let (data, _old_selection) = scan_data.scan_files.into_parts();
                let filtered_files = FilteredEngineData::try_new(data, selection_vector)?;

                txn.remove_files(filtered_files);
            }
        }

        // Commit the transaction
        let _committed = match txn.commit(&engine)? {
            CommitResult::CommittedTransaction(c) => c,
            _ => panic!("Transaction should succeed"),
        };

        // Step 6: Scan again to verify file was removed
        let final_files: Vec<String> = get_files_from_scan(&table_root, &engine)?
            .into_iter()
            .filter(|f| !f.contains(".content.")) // Filter out ContentRoot metadata files
            .collect();
        assert_eq!(final_files.len(), 4, "Expected 4 data files after removal");

        // Verify that exactly one file was removed
        let removed_count = initial_files
            .iter()
            .filter(|f| !final_files.contains(f))
            .count();
        assert_eq!(removed_count, 1, "Expected exactly 1 file to be removed");

        // Verify that file-2.parquet (the one with DV) is no longer present
        assert!(
            !final_files.iter().any(|f| f.contains("file-2.parquet")),
            "file-2.parquet should have been removed"
        );

        // Step 7: Verify the ManifestDV for the delete manifest
        // When delete_from_leaf is used, it creates a ManifestDV entry that marks which
        // indices in the leaf manifest are deleted, without rewriting the leaf file.
        use crate::content_tree::ContentTreeNode;

        // Read the ContentRoot action from version 2 to get the new manifest path
        let delta_log_path = table_root
            .join("_delta_log/")?
            .to_file_path()
            .map_err(|_| Error::generic("Cannot convert URL to file path"))?;
        let v2_log_file = delta_log_path.join("00000000000000000002.json");
        let v2_log_content = std::fs::read_to_string(&v2_log_file)
            .map_err(|e| Error::generic(format!("Failed to read v2 log: {e}")))?;

        // Parse the ContentRoot action to get the manifest path
        let content_root_path: String = v2_log_content
            .lines()
            .find_map(|line| {
                serde_json::from_str::<serde_json::Value>(line)
                    .ok()
                    .and_then(|v| {
                        v.get("contentRoot")?
                            .get("path")?
                            .as_str()
                            .map(String::from)
                    })
            })
            .ok_or_else(|| Error::generic("No ContentRoot found in v2"))?;

        // Read the root manifest (path is now relative, so join with table root)
        let root_manifest_url = table_root
            .join(&content_root_path)
            .map_err(|e| Error::generic(format!("Failed to parse manifest URL: {e}")))?;
        let (iter, version, path_in_log) = ContentTreeNode::open_stream(
            engine.parquet_handler(),
            &root_manifest_url,
            content_root_path.clone(),
            None,
            None,
        )?;
        let data = iter.collect::<DeltaResult<Vec<_>>>()?;
        let root_metadata = ContentTreeNode::from_batches_with_version(
            data,
            version,
            path_in_log,
            table_root.clone(),
        )?;
        let root_entries = root_metadata.entries()?;

        // In the new CombinedManifest model, the data leaf (with inline DVs) is a CombinedManifest.
        // After file removal, the CombinedManifest entry in the root should have a manifest_dv
        // marking which data file entry indices are deleted.
        let manifest_with_dv = root_entries
            .iter()
            .find(|entry| {
                entry.content_type == DataContentType::CombinedManifest
                    && entry.manifest_dv.is_some()
            })
            .ok_or_else(|| {
                Error::generic(
                    "No CombinedManifest with manifest_dv found in root after file removal",
                )
            })?;

        let leaf_manifest_path = manifest_with_dv
            .location
            .clone()
            .ok_or_else(|| Error::generic("CombinedManifest has no location"))?;

        let manifest_dv_bytes = manifest_with_dv
            .manifest_dv
            .as_ref()
            .ok_or_else(|| Error::generic("CombinedManifest has no manifest_dv"))?;

        if manifest_dv_bytes.len() < 4 {
            return Err(Box::new(Error::generic("manifest_dv bytes too short")));
        }

        // Decode the roaring bitmap to verify the deleted index
        use roaring::RoaringTreemap;
        let inline_content = manifest_dv_bytes;
        let deleted_indices =
            RoaringTreemap::deserialize_from(&inline_content[4..]).map_err(|e| {
                Box::new(Error::generic(format!(
                    "Failed to deserialize ManifestDV: {e}"
                ))) as Box<dyn std::error::Error>
            })?;

        assert_eq!(
            deleted_indices.len(),
            1,
            "ManifestDV should mark exactly 1 index as deleted"
        );

        let deleted_index = deleted_indices.iter().next().unwrap();

        // Read the leaf manifest and verify the entry at the deleted index
        let leaf_manifest_url = table_root
            .join(&leaf_manifest_path)
            .map_err(|e| Error::generic(format!("Failed to parse leaf manifest URL: {e}")))?;
        let (iter, version, path_in_log) = ContentTreeNode::open_stream(
            engine.parquet_handler(),
            &leaf_manifest_url,
            leaf_manifest_path.clone(),
            None,
            None,
        )?;
        let data = iter.collect::<DeltaResult<Vec<_>>>()?;
        let delete_manifest_metadata = ContentTreeNode::from_batches_with_version(
            data,
            version,
            path_in_log,
            table_root.clone(),
        )?;
        let delete_entries = delete_manifest_metadata.entries()?;

        // Get the Data entry at the deleted index
        let deleted_data_entry = delete_entries
            .get(deleted_index as usize)
            .ok_or_else(|| Error::generic(format!("No entry at index {}", deleted_index)))?;

        // In the new model, it's a Data entry (location is the data file path)
        assert_eq!(
            deleted_data_entry.content_type,
            DataContentType::Data,
            "Deleted entry should be a Data entry"
        );

        assert!(
            deleted_data_entry
                .location
                .as_ref()
                .is_some_and(|f| f.contains("file-2.parquet")),
            "Deleted Data entry should be for file-2.parquet"
        );

        Ok(())
    }

    #[test]
    fn test_content_root_version_matches_commit() -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use std::fs::read_dir;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        // Canonicalize the path to match what try_parse_uri does in real usage
        // This ensures paths are consistent (e.g., /private/var instead of /var on macOS)
        let canonical_path = std::fs::canonicalize(temp_dir.path())?;
        let table_root = Url::from_directory_path(canonical_path).unwrap();

        // Step 1: Create initial table (v0) with Protocol + Metadata for content root support
        // Create a table with metadataTree-experimental feature and column mapping
        use serde_json::json;
        use std::fs::{create_dir_all, write};
        use uuid::Uuid;

        let table_id = Uuid::new_v4().to_string();
        let schema = json!({
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": true,
                    "metadata": {
                        "PARQUET:field_id": 1,
                        "delta.columnMapping.id": 1,
                        "delta.columnMapping.physicalName": "id"
                    }
                },
                {
                    "name": "value",
                    "type": "string",
                    "nullable": true,
                    "metadata": {
                        "PARQUET:field_id": 2,
                        "delta.columnMapping.id": 2,
                        "delta.columnMapping.physicalName": "value"
                    }
                }
            ]
        });

        let protocol = json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["columnMapping", "metadataTree-experimental"],
                "writerFeatures": ["columnMapping", "metadataTree-experimental"]
            }
        });

        let metadata = json!({
            "metaData": {
                "id": table_id,
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": schema.to_string(),
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": "id"
                },
                "createdTime": 1677811175819u64
            }
        });

        let data = [
            serde_json::to_vec(&protocol)?,
            b"\n".to_vec(),
            serde_json::to_vec(&metadata)?,
        ]
        .concat();

        let delta_log_path = table_root
            .join("_delta_log/")?
            .to_file_path()
            .map_err(|_| Error::generic("Cannot convert URL to file path"))?;

        create_dir_all(&delta_log_path)
            .map_err(|e| Error::generic(format!("Failed to create _delta_log: {e}")))?;

        let file_path = delta_log_path.join("00000000000000000000.json");
        write(&file_path, data)
            .map_err(|e| Error::generic(format!("Failed to write initial log: {e}")))?;

        // Step 2: Create snapshot and transaction in batch_commit mode
        let snapshot = crate::Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let committer = Box::new(FileSystemCommitter::new());
        let mut txn = snapshot
            .transaction(committer, &engine)?
            .with_batch_commit()
            .with_operation("CREATE_CONTENT_ROOT".to_string());

        // Step 3: Release root and delta actions
        let scan = txn.release_root_and_delta_actions()?;

        // Step 4: Create leaf writers and add files
        let mut leaf1 = txn.new_leaf_node_writer(&engine)?;
        let mut leaf2 = txn.new_leaf_node_writer(&engine)?;

        // Helper to create add metadata for testing
        // Note: stats are set to null (empty struct) because proper content_stats requires
        // matching the table schema's stats format
        fn create_test_add_metadata(paths: Vec<&str>) -> DeltaResult<Box<dyn crate::EngineData>> {
            use crate::arrow::array::{ArrayRef, Int64Array, MapArray, StringArray, StructArray};
            use crate::arrow::buffer::{NullBuffer, OffsetBuffer};
            use crate::arrow::datatypes::{DataType as ArrowDataType, Field};
            use crate::arrow::record_batch::RecordBatch;
            use crate::engine::arrow_data::ArrowEngineData;
            use crate::schema::{DataType, MapType, StructField, StructType};

            let num_files = paths.len();

            // Create schema with empty stats struct
            let schema = Arc::new(StructType::new_unchecked(vec![
                StructField::not_null("path", DataType::STRING),
                StructField::not_null(
                    "partitionValues",
                    DataType::Map(Box::new(MapType::new(
                        DataType::STRING,
                        DataType::STRING,
                        true,
                    ))),
                ),
                StructField::not_null("size", DataType::LONG),
                StructField::not_null("modificationTime", DataType::LONG),
                StructField::nullable("stats", DataType::struct_type_unchecked(vec![])),
            ]));

            let arrow_schema = Arc::new(
                crate::engine::arrow_conversion::TryIntoArrow::try_into_arrow(schema.as_ref())?,
            );

            // Create arrays
            let paths_array: ArrayRef =
                Arc::new(StringArray::from_iter_values(paths.iter().copied()));
            let size_array: ArrayRef =
                Arc::new(Int64Array::from_iter_values(vec![1024; num_files]));
            let mod_time_array: ArrayRef =
                Arc::new(Int64Array::from_iter_values(vec![1000000; num_files]));

            // Empty partition values
            let entries_field = Arc::new(Field::new(
                "key_value",
                ArrowDataType::Struct(
                    vec![
                        Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                        Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            ));
            let empty_keys = StringArray::from(Vec::<&str>::new());
            let empty_values = StringArray::from(Vec::<Option<&str>>::new());
            let empty_entries = StructArray::from(vec![
                (
                    Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                    Arc::new(empty_keys) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                    Arc::new(empty_values) as ArrayRef,
                ),
            ]);
            let offsets = OffsetBuffer::from_lengths(vec![0; num_files]);
            let partition_values: ArrayRef = Arc::new(MapArray::new(
                entries_field,
                offsets,
                empty_entries,
                None,
                false,
            ));

            // All-null stats (empty struct with null buffer)
            let stats: ArrayRef = Arc::new(StructArray::new_empty_fields(
                num_files,
                Some(NullBuffer::from(vec![false; num_files])),
            ));

            let record_batch = RecordBatch::try_new(
                arrow_schema,
                vec![
                    paths_array,
                    partition_values,
                    size_array,
                    mod_time_array,
                    stats,
                ],
            )?;

            Ok(Box::new(ArrowEngineData::new(record_batch)))
        }

        // Add files to leaf1
        let leaf1_metadata = create_test_add_metadata(vec![
            "leaf1-file-0.parquet",
            "leaf1-file-1.parquet",
            "leaf1-file-2.parquet",
        ])?;
        leaf1.add_files(&engine, leaf1_metadata)?;

        // Add files to leaf2
        let leaf2_metadata =
            create_test_add_metadata(vec!["leaf2-file-0.parquet", "leaf2-file-1.parquet"])?;
        leaf2.add_files(&engine, leaf2_metadata)?;

        // Step 5: Finish leaf writers and add to transaction
        txn.add_leaf(leaf1.finish(&engine)?)?;
        txn.add_leaf(leaf2.finish(&engine)?)?;

        // Exhaust the scan (required before commit)
        for _ in scan.scan_metadata(&engine)? {}

        // Step 6: Commit the transaction (this should be version 1)
        let committed = match txn.commit(&engine)? {
            CommitResult::CommittedTransaction(c) => c,
            _ => panic!("Transaction should succeed"),
        };

        assert_eq!(
            committed.commit_version(),
            1,
            "Commit should be at version 1"
        );

        // Step 7: Validate that all content manifest files are at version 1
        let delta_log_path = table_root
            .join("_delta_log/")?
            .to_file_path()
            .map_err(|_| Error::generic("Cannot convert URL to file path"))?;

        let mut content_files: Vec<String> = read_dir(&delta_log_path)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.contains(".content.") && name.ends_with(".parquet"))
            .collect();

        content_files.sort();

        // Should have exactly 3 content files: 1 root + 2 leaves
        assert_eq!(
            content_files.len(),
            3,
            "Expected 3 content manifest files (1 root + 2 leaves)"
        );

        // All content files should be at version 1 (00000000000000000001)
        for file_name in &content_files {
            assert!(
                file_name.starts_with("00000000000000000001.content."),
                "Content file {} should be at version 1",
                file_name
            );
        }

        // Step 8: Validate that the root manifest file exists (without UUID in name)
        let root_manifest_exists = content_files
            .iter()
            .any(|name| name == "00000000000000000001.content.parquet");

        assert!(
            root_manifest_exists,
            "Root manifest at version 1 should exist: 00000000000000000001.content.parquet"
        );

        // Step 9: Validate that the ContentRoot action points to the correct root manifest
        use serde_json::Value;
        use std::fs::read_to_string;

        let commit_1_path = delta_log_path.join("00000000000000000001.json");
        let commit_content = read_to_string(&commit_1_path)?;

        // Parse each line as JSON and find the contentRoot action
        let mut found_content_root = false;
        for line in commit_content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let action: Value = serde_json::from_str(line)?;
            if let Some(content_root) = action.get("contentRoot") {
                if let Some(path) = content_root.get("path").and_then(|p| p.as_str()) {
                    // The path should reference the version 1 root manifest
                    assert!(
                        path.contains("00000000000000000001.content.parquet"),
                        "ContentRoot action should reference version 1 root manifest, got: {}",
                        path
                    );

                    // sizeInBytes should match the actual file size on disk
                    let reported_size = content_root
                        .get("sizeInBytes")
                        .and_then(|s| s.as_u64())
                        .expect("ContentRoot sizeInBytes should be present");
                    let manifest_file = table_root
                        .join(path)
                        .expect("should join path")
                        .to_file_path()
                        .expect("should be a local path");
                    let disk_size = manifest_file
                        .metadata()
                        .expect("manifest file should exist")
                        .len();
                    assert!(
                        reported_size > 0,
                        "ContentRoot sizeInBytes should be non-zero"
                    );
                    assert_eq!(
                        reported_size, disk_size,
                        "ContentRoot sizeInBytes should match actual file size"
                    );

                    found_content_root = true;
                    break;
                }
            }
        }

        assert!(
            found_content_root,
            "ContentRoot action should exist in version 1 commit"
        );

        Ok(())
    }

    #[test]
    fn test_commit_io_error_returns_retryable_transaction() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut txn = snapshot.transaction(Box::new(IoErrorCommitter), engine.as_ref())?;
        add_dummy_file(&mut txn);
        let result = txn.commit(engine.as_ref())?;
        assert!(
            matches!(result, CommitResult::RetryableTransaction(_)),
            "Expected RetryableTransaction, got: {result:?}"
        );
        if let CommitResult::RetryableTransaction(retryable) = result {
            assert!(
                retryable.error.to_string().contains("simulated IO error"),
                "Unexpected error: {}",
                retryable.error
            );
        }
        Ok(())
    }

    #[test]
    fn test_existing_table_txn_debug() -> DeltaResult<()> {
        let (_engine, txn, _tempdir) = create_existing_table_txn()?;
        let debug_str = format!("{:?}", txn);
        // Existing-table transactions should include the snapshot version number
        assert!(
            debug_str.contains("Transaction") && debug_str.contains("read_snapshot version"),
            "Debug output should contain Transaction info: {debug_str}"
        );
        // Should NOT contain "create_table"
        assert!(
            !debug_str.contains("create_table"),
            "Existing table debug should not contain create_table: {debug_str}"
        );
        Ok(())
    }

    // Input schemas have no CM metadata; create_table automatically assigns IDs and
    // physical names when mode is Name or Id.
    #[rstest]
    #[case::flat_none(test_schema_flat(), ColumnMappingMode::None)]
    #[case::flat_name(test_schema_flat(), ColumnMappingMode::Name)]
    #[case::flat_id(test_schema_flat(), ColumnMappingMode::Id)]
    #[case::nested_none(test_schema_nested(), ColumnMappingMode::None)]
    #[case::nested_name(test_schema_nested(), ColumnMappingMode::Name)]
    #[case::nested_id(test_schema_nested(), ColumnMappingMode::Id)]
    #[case::map_none(test_schema_with_map(), ColumnMappingMode::None)]
    #[case::map_name(test_schema_with_map(), ColumnMappingMode::Name)]
    #[case::map_id(test_schema_with_map(), ColumnMappingMode::Id)]
    #[case::array_none(test_schema_with_array(), ColumnMappingMode::None)]
    #[case::array_name(test_schema_with_array(), ColumnMappingMode::Name)]
    #[case::array_id(test_schema_with_array(), ColumnMappingMode::Id)]
    fn test_physical_schema_column_mapping(
        #[case] schema: SchemaRef,
        #[case] mode: ColumnMappingMode,
    ) -> DeltaResult<()> {
        let (_engine, txn) = crate::utils::test_utils::setup_column_mapping_txn(schema, mode)?;
        let write_context = txn.get_write_context();
        crate::utils::test_utils::validate_physical_schema_column_mapping(
            write_context.logical_schema(),
            write_context.physical_schema(),
            mode,
        );
        Ok(())
    }

    /// Builds a RecordBatch with logical field names matching [`test_schema_nested`].
    fn build_test_record_batch() -> DeltaResult<RecordBatch> {
        let arrow_schema: ArrowSchema = test_schema_nested().as_ref().try_into_arrow()?;

        let id_arr: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2]));

        // info struct fields
        let name_arr: ArrayRef = Arc::new(StringArray::from(vec!["alice", "bob"]));
        let age_arr: ArrayRef = Arc::new(Int32Array::from(vec![30, 25]));

        // info.tags: Map<String, String>
        let keys = StringArray::from(vec!["k1", "k2"]);
        let vals = StringArray::from(vec!["v1", "v2"]);
        let entries_field = ArrowField::new(
            "key_value",
            ArrowDataType::Struct(
                vec![
                    ArrowField::new("key", ArrowDataType::Utf8, false),
                    ArrowField::new("value", ArrowDataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        );
        let entries = StructArray::try_new(
            vec![
                ArrowField::new("key", ArrowDataType::Utf8, false),
                ArrowField::new("value", ArrowDataType::Utf8, true),
            ]
            .into(),
            vec![Arc::new(keys), Arc::new(vals)],
            None,
        )?;
        let map_offsets = crate::arrow::buffer::OffsetBuffer::new(vec![0i32, 1, 2].into());
        let tags_arr: ArrayRef = Arc::new(MapArray::new(
            Arc::new(entries_field),
            map_offsets,
            entries,
            None,
            false,
        ));

        // info.scores: Array<Int>
        let score_values = Int32Array::from(vec![10, 20, 30]);
        let offsets = crate::arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 3].into());
        let scores_arr: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(ArrowField::new("element", ArrowDataType::Int32, true)),
            offsets,
            Arc::new(score_values),
            None,
        )?);

        // info struct
        let info_fields = vec![
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("age", ArrowDataType::Int32, true),
            ArrowField::new("tags", tags_arr.data_type().clone(), true),
            ArrowField::new("scores", scores_arr.data_type().clone(), true),
        ];
        let info_arr: ArrayRef = Arc::new(StructArray::try_new(
            info_fields.into(),
            vec![name_arr, age_arr, tags_arr, scores_arr],
            None,
        )?);

        Ok(RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![id_arr, info_arr],
        )?)
    }

    /// Validates that [`WriteContext::logical_to_physical`] correctly renames fields at all nesting levels.
    /// Builds a RecordBatch with logical names, evaluates the transform, and checks that the
    /// output uses physical names from the physical schema — including nested struct children.
    fn validate_logical_to_physical_transform(mode: ColumnMappingMode) -> DeltaResult<()> {
        let schema = test_schema_nested();
        let (_engine, txn) = crate::utils::test_utils::setup_column_mapping_txn(schema, mode)?;
        let write_context = txn.get_write_context();
        let logical_schema = write_context.logical_schema();
        let physical_schema = write_context.physical_schema();
        let logical_to_physical_expression = write_context.logical_to_physical();

        if mode != ColumnMappingMode::None {
            assert_ne!(
                logical_schema, physical_schema,
                "Physical schema should differ from logical schema when column mapping is enabled"
            );
        }

        let batch = build_test_record_batch()?;

        // Evaluate the logical_to_physical expression
        let input_schema: SchemaRef = logical_schema.clone();
        let handler = ArrowEvaluationHandler;
        let evaluator = handler.new_expression_evaluator(
            input_schema,
            logical_to_physical_expression.clone(),
            physical_schema.clone().into(),
        )?;
        let result = evaluator.evaluate(&ArrowEngineData::new(batch))?;
        let result = ArrowEngineData::try_from_engine_data(result)?;
        let result_batch = result.record_batch();

        // Verify: all field names, types, and metadata match the physical schema
        let expected_arrow_schema: ArrowSchema = physical_schema.as_ref().try_into_arrow()?;
        assert_eq!(result_batch.schema().as_ref(), &expected_arrow_schema);

        // Verify: data is preserved (id values)
        let id_col = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64");
        assert_eq!(id_col.values(), &[1i64, 2]);

        Ok(())
    }

    #[rstest]
    #[case::name_mode(ColumnMappingMode::Name)]
    #[case::id_mode(ColumnMappingMode::Id)]
    #[case::none_mode(ColumnMappingMode::None)]
    fn test_logical_to_physical_transform(#[case] mode: ColumnMappingMode) -> DeltaResult<()> {
        validate_logical_to_physical_transform(mode)
    }

    #[rstest]
    #[case::dropped("./tests/data/basic_partitioned/", 2, &[])]
    #[case::kept("./tests/data/partitioned_with_materialize_feature/", 3, &["letter"])]
    fn test_partition_column_in_eval_output(
        #[case] table_path: &str,
        #[case] expected_cols: usize,
        #[case] expected_partition_cols: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::arrow::array::Float64Array;
        let (_snap, wc) = snapshot_and_write_context(table_path)?;
        let batch = RecordBatch::try_new(
            Arc::new(wc.logical_schema().as_ref().try_into_arrow()?),
            vec![
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Float64Array::from(vec![1.5])),
            ],
        )?;
        let rb = eval_logical_to_physical(&wc, batch)?;
        assert_eq!(rb.num_columns(), expected_cols);
        for col in expected_partition_cols {
            assert!(rb.schema().fields().iter().any(|f| f.name() == *col));
        }
        Ok(())
    }
}
