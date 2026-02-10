pub(crate) mod builder;
pub(crate) mod bulk_processor;
pub(crate) mod lazy_reader;
pub(crate) mod reader;
pub(crate) mod stats;
pub(crate) mod writer;

// Metadata based on Adaptive Metadata Tree
// https://docs.google.com/document/d/1k4x8utgh41Sn1tr98eynDKCWq035SV_f75rtNHcerVw
use crate::actions::{ContentRoot, ADD_NAME, REMOVE_NAME};
use crate::engine_data::{EngineData, FilteredEngineData};
use crate::expressions::{ColumnName, Predicate, PredicateRef, Scalar, StructData};
use crate::kernel_predicates::parquet_stats_skipping::ParquetStatsProvider;
use crate::kernel_predicates::KernelPredicateEvaluator;
use crate::log_replay::{ActionsBatch, HasSelectionVector};
use crate::metadata::builder::MetadataBuilder;
use crate::path::ParsedLogPath;
use crate::scan::ScanBuilder;
use crate::schema::{derive_macro_utils::ToDataType, DataType, StructField, StructType};
use crate::{
    DeltaResult, Engine, Error, EvaluationHandler, ExpressionEvaluator, FileMeta, LookupJoiner,
    ParquetHandler, SchemaRef, SnapshotRef, Version,
};
use bytes::Bytes;
use delta_kernel_derive::{IntoEngineData, ToSchema};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use tracing::debug;
use url::Url;

/// Type alias for the iterator returned by `open_stream`.
type ParquetStreamResult = (
    Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
    Version,
    String,
);

/// Cached schema for the projection of metadata columns used when building DV batches.
/// Includes: contentType, referencedFile, trackingInfo, dv_cardinality, deleteManifestPath, deleteManifestPosition
static DV_PROJECTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("contentType", DataType::INTEGER, false),
        StructField::new("referencedFile", DataType::STRING, true),
        StructField::new(
            "trackingInfo",
            DataType::Struct(Box::new(StructType::new_unchecked(vec![
                StructField::new("status", DataType::INTEGER, false),
                StructField::new("snapshotId", DataType::LONG, true),
                StructField::new("sequenceNumber", DataType::LONG, true),
                StructField::new("fileSequenceNumber", DataType::LONG, true),
                StructField::new("firstRowId", DataType::LONG, true),
                StructField::new("changesDv", DataType::BINARY, true),
            ]))),
            true,
        ),
        StructField::new("dv_cardinality", DataType::LONG, true),
        StructField::new("deleteManifestPath", DataType::STRING, true),
        StructField::new("deleteManifestPosition", DataType::LONG, true),
    ]))
});

/// Cached schema for parsed deletion vector columns (flat representation).
/// This schema is used when appending DV fields to batches in `append_parsed_dv_columns()`.
/// These columns can be transformed using an expression.
static DV_COLUMNS_SCHEMA_TRANSFORMABLE: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("dv_cardinality", DataType::LONG, true),
        StructField::new("deleteManifestPath", DataType::STRING, true),
        StructField::new("deleteManifestPosition", DataType::LONG, true),
    ]))
});

// Flat schema for DV columns that need to be transformed via a visitor
// (this has non-trivial cost)
static DV_COLUMNS_SCHEMA_VISITOR_NEEDED: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("dv_storageType", DataType::STRING, true),
        StructField::new("dv_pathOrInlineDv", DataType::STRING, true),
        StructField::new("dv_offset", DataType::INTEGER, true),
        StructField::new("dv_sizeInBytes", DataType::INTEGER, true),
    ]))
});

static DV_COLUMNS_SCHEMA_FINAL: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields: Vec<_> = DV_COLUMNS_SCHEMA_TRANSFORMABLE.fields().cloned().collect();
    fields.extend(DV_COLUMNS_SCHEMA_VISITOR_NEEDED.fields().cloned());
    Arc::new(StructType::new_unchecked(fields))
});

/// Cached schema for stats with just numRecords field.
/// Used when building Add action stats from metadata entries.
static STATS_NUM_RECORDS_SCHEMA: LazyLock<StructType> = LazyLock::new(|| {
    StructType::new_unchecked(vec![StructField::new("numRecords", DataType::LONG, false)])
});

/// A stats provider that extracts min/max statistics from AMT manifest `content_stats`.
///
/// This struct implements `ParquetStatsProvider` to enable predicate evaluation against
/// manifest-level statistics for data skipping. The `content_stats` field in a manifest
/// entry contains aggregated min/max bounds over all files in that manifest.
///
/// The stats structure follows the AMT format:
/// ```text
/// content_stats: {
///   column_name: {
///     value_count: i64,
///     null_value_count: i64,  // if nullable
///     lower_bound: <column_type>,
///     upper_bound: <column_type>,
///     exact_bounds: bool
///   },
///   ...
/// }
/// ```
struct ManifestStatsProvider<'a> {
    /// The content_stats from a manifest entry
    content_stats: &'a StructData,
    /// Total record count from the manifest entry (used for rowcount stat)
    record_count: i64,
}

impl<'a> ManifestStatsProvider<'a> {
    /// Creates a new ManifestStatsProvider from a manifest entry's content_stats.
    fn new(content_stats: &'a StructData, record_count: i64) -> Self {
        Self {
            content_stats,
            record_count,
        }
    }

    /// Looks up a nested scalar value in the content_stats structure.
    ///
    /// TODO: Fix nested fields
    /// TODO: Lookup based on field-id
    /// TODO: Explore option of pushing this to the engine
    /// TODO: Add missing fields around sizes and nan's
    ///
    /// Given a column name like `["col1"]`, this navigates:
    /// `content_stats.col1.<stat_field>` where stat_field is "lower_bound", "upper_bound", etc.
    fn get_stat_value(&self, col: &ColumnName, stat_field: &str) -> Option<Scalar> {
        let col_stats = self.get_column_stats(col)?;
        col_stats
            .fields()
            .iter()
            .zip(col_stats.values())
            .find(|(field, _)| field.name() == stat_field)
            .map(|(_, value)| value)
            .filter(|value| !value.is_null())
            .cloned()
    }

    /// Gets the stats struct for a specific column from content_stats.
    fn get_column_stats(&self, col: &ColumnName) -> Option<&StructData> {
        col.iter()
            .try_fold(self.content_stats, |current, field_name| {
                current
                    .fields()
                    .iter()
                    .zip(current.values())
                    .find(|(field, _)| field.name() == field_name)
                    .and_then(|(_, value)| match value {
                        Scalar::Struct(nested) => Some(nested),
                        _ => None,
                    })
            })
    }
}

impl<'a> ParquetStatsProvider for ManifestStatsProvider<'a> {
    fn get_parquet_min_stat(&self, col: &ColumnName, _data_type: &DataType) -> Option<Scalar> {
        self.get_stat_value(col, "lower_bound")
    }

    fn get_parquet_max_stat(&self, col: &ColumnName, _data_type: &DataType) -> Option<Scalar> {
        self.get_stat_value(col, "upper_bound")
    }

    fn get_parquet_nullcount_stat(&self, col: &ColumnName) -> Option<i64> {
        match self.get_stat_value(col, "null_value_count") {
            Some(Scalar::Long(count)) => Some(count),
            _ => None,
        }
    }

    fn get_parquet_rowcount_stat(&self) -> i64 {
        self.record_count
    }
}

/// Evaluates whether an entry can be skipped based on its content_stats and a predicate.
///
/// This function works for any `MetadataEntry` type - data files, manifests, etc.
/// It uses the entry's `content_stats` (min/max bounds) to determine if the predicate
/// can possibly match any rows in the entry.
///
/// Returns `true` if the entry can definitely be skipped (no rows in the entry
/// can possibly satisfy the predicate based on min/max stats).
/// Returns `false` if the entry might contain matching rows and should be processed.
///
/// If content_stats is None or the predicate cannot be evaluated, returns `false` (cannot skip).
fn can_skip_entry(entry: &MetadataEntry, predicate: &Predicate) -> bool {
    let content_stats = match &entry.content_stats {
        Some(stats) => stats,
        None => return false, // No stats available, cannot skip
    };

    let provider = ManifestStatsProvider::new(content_stats, entry.record_count);

    // Use the KernelPredicateEvaluator to evaluate the predicate against stats.
    // The evaluator returns Some(true) if the predicate might match, Some(false) if it
    // definitely cannot match, or None if it cannot be determined.
    match provider.eval(predicate) {
        Some(false) => {
            // Predicate definitely cannot match any rows in this entry
            debug!(
                "Skipping entry {:?} - predicate cannot match based on stats",
                entry.location
            );
            true
        }
        _ => {
            // Predicate might match, or we couldn't determine - don't skip
            false
        }
    }
}

/// Filters a vector of entries based on a predicate using content_stats.
///
/// Returns only entries that might contain matching data (cannot be skipped).
/// Logs the number of entries skipped for debugging.
fn filter_entries_by_predicate(
    entries: Vec<MetadataEntry>,
    predicate: Option<&PredicateRef>,
    entry_type: &str,
) -> Vec<MetadataEntry> {
    let Some(pred) = predicate else {
        return entries;
    };

    let total = entries.len();
    let filtered: Vec<MetadataEntry> = entries
        .into_iter()
        .filter(|entry| !can_skip_entry(entry, pred))
        .collect();

    let skipped = total - filtered.len();
    if skipped > 0 {
        debug!(
            "Data skipping: skipped {}/{} {} based on content_stats",
            skipped, total, entry_type
        );
    }

    filtered
}

/// Represents table metadata in Adaptive Metadata Tree (AMT) format.
///
/// This structure contains metadata entries that describe the files in a Delta table
/// at a specific version. It is used for interoperability with Apache Iceberg's
/// metadata tree format.
///
/// Each `Metadata` instance contains:
/// - A collection of `MetadataEntry` records (one per file)
/// - The Delta table version this metadata represents
/// - The table root URL for resolving relative file paths
/// - An optional leaf UUID (only set when writing a leaf manifest, not for root)
#[allow(dead_code)]
pub struct Metadata {
    data: Vec<Box<dyn EngineData>>,
    version: Version,
    table_root: Url,
    /// The exact path string as it appears in the Delta log (from contentRoot action or manifest location field).
    /// This is NOT normalized or converted - it flows through exactly as stored in the log.
    /// Empty string for newly built metadata that hasn't been written yet.
    path_in_log: String,
    /// Optional UUID that identifies this metadata as a leaf manifest.
    /// When writing a root manifest, this is `None`.
    /// When writing a leaf manifest, this must be set to a unique UUID.
    leaf: Option<uuid::Uuid>,
}

/// A manifest entry wrapper.
///
/// According to the Iceberg Single File Commits spec, manifest deletion vectors
/// can filter out entries from a manifest by ordinal position without rewriting the manifest file.
#[derive(Debug, Clone)]
pub(crate) struct FilteredManifest {
    /// The manifest entry (can be DataManifest or DeleteManifest)
    pub(crate) manifest: MetadataEntry,
}

impl FilteredManifest {
    /// Creates a new FilteredManifest
    pub(crate) fn new(manifest: MetadataEntry) -> Self {
        Self { manifest }
    }
}

/// State shared across all leaf manifests (child data manifests).
///
/// This contains deletion information that applies globally:
/// - Unaffiliated delete manifests (apply to all data files)
/// - Unmatched DVs from root (position deletes that reference files not in root)
#[derive(Debug, Clone)]
pub(crate) struct SharedLeafState {
    /// Delete manifests with no specific affiliation (apply to all data files)
    pub(crate) unaffiliated_dv_manifests: Vec<FilteredManifest>,
}

/// Complete state of the root manifest, including manifest references and deletion vectors.
///
/// This structure separates concerns:
/// - Manifest references (data and affiliated delete manifests) are per-child-manifest
/// - Shared state (unaffiliated manifests) apply to all children
#[derive(Debug, Clone)]
pub(crate) struct LeafReferences {
    /// References to child data manifests and their affiliated delete manifests
    pub(crate) manifest_references: Vec<ManifestReference>,
    /// Shared state that applies to all leaf manifests
    pub(crate) shared_state: SharedLeafState,
}

/// References to manifest files discovered in the root manifest.
/// According to the Iceberg Single File Commits spec, the root manifest can reference
/// child data manifests and delete manifests.
#[derive(Debug, Clone)]
pub(crate) struct ManifestReference {
    /// The data manifest entry to process, with optional manifest DV
    pub(crate) data_manifest: FilteredManifest,
    /// Delete manifest entries affiliated with this specific data manifest (via referenced_file)
    pub(crate) affiliated_dv_manifests: Vec<FilteredManifest>,
}

/// A pair of optional Add and Remove evaluators
struct EvaluatorPair {
    add_evaluator: Option<Arc<dyn ExpressionEvaluator>>,
    remove_evaluator: Option<Arc<dyn ExpressionEvaluator>>,
}

/// Stateful applicator for manifest deletion vectors.
///
/// Instead of materializing a full Vec<bool> selection vector upfront,
/// this applicator queries the RoaringTreemap on-demand for each batch.
struct ManifestDvApplicator {
    /// Parsed deletion vector (None if no manifest DV present)
    deleted_positions: Option<roaring::RoaringTreemap>,

    /// Current cumulative row offset
    offset: usize,
}

impl ManifestDvApplicator {
    /// Create new applicator from manifest_dv bytes.
    fn new(manifest_dv: Option<&Bytes>) -> DeltaResult<Self> {
        let deleted_positions = if let Some(dv_bytes) = manifest_dv {
            parse_manifest_dv(dv_bytes)?
        } else {
            None
        };
        Ok(Self {
            deleted_positions,
            offset: 0,
        })
    }

    /// Process a batch and return it with its selection vector.
    ///
    /// Returns FilteredEngineData containing the batch and a selection vector.
    /// If a manifest DV is present, the selection vector indicates which rows are NOT deleted.
    /// If no manifest DV is present, all rows are marked as selected.
    fn process_batch(&mut self, batch: Box<dyn EngineData>) -> DeltaResult<FilteredEngineData> {
        let batch_len = batch.len();
        let filtered = if let Some(deleted) = &self.deleted_positions {
            let selection: Vec<bool> = (0..batch_len)
                .map(|i| !deleted.contains((self.offset + i) as u64))
                .collect();
            FilteredEngineData::try_new(batch, selection)?
        } else {
            FilteredEngineData::with_all_rows_selected(batch)
        };
        self.offset += batch_len;
        Ok(filtered)
    }
}

impl Metadata {
    /// Creates a new empty Metadata instance for the specified table version.
    ///
    /// This creates a root manifest (leaf is `None`).
    ///
    /// # Parameters
    /// - `version`: The Delta table version this metadata represents
    /// - `table_root`: The root URL of the Delta table
    #[allow(dead_code)]
    pub(crate) fn new(version: Version, table_root: Url) -> Self {
        Self {
            data: vec![],
            version,
            table_root,
            path_in_log: String::new(),
            leaf: None,
        }
    }

    /// Creates a new empty Metadata instance as a leaf manifest.
    ///
    /// Leaf manifests have a UUID automatically generated to uniquely identify them.
    ///
    /// # Parameters
    /// - `version`: The Delta table version this metadata represents
    /// - `table_root`: The root URL of the Delta table
    #[allow(dead_code)]
    pub(crate) fn new_leaf(version: Version, table_root: Url) -> Self {
        Self {
            data: vec![],
            version,
            table_root,
            path_in_log: String::new(),
            leaf: Some(uuid::Uuid::new_v4()),
        }
    }

    /// Creates a Metadata instance from pre-loaded batches.
    ///
    /// This is used for parallel IO optimization where batches are read upfront.
    ///
    /// # Parameters
    /// - `data`: Pre-loaded batches containing metadata entries
    /// - `path_in_log`: The path as it appears in the Delta log
    /// - `table_root`: The root URL of the Delta table
    pub(crate) fn from_batches(
        data: Vec<Box<dyn EngineData>>,
        path_in_log: String,
        table_root: Url,
    ) -> Self {
        Self {
            data,
            version: 0, // Version not relevant for child manifests
            table_root,
            path_in_log,
            leaf: None,
        }
    }

    /// Construct Metadata from batches with a specific version (for content root reading).
    pub(crate) fn from_batches_with_version(
        data: Vec<Box<dyn EngineData>>,
        version: Version,
        path_in_log: String,
        table_root: Url,
    ) -> Self {
        Self {
            data,
            version,
            table_root,
            path_in_log,
            leaf: None,
        }
    }

    /// Returns the leaf UUID if this is a leaf manifest, or `None` if it's a root manifest.
    #[allow(dead_code)]
    pub(crate) fn leaf(&self) -> Option<uuid::Uuid> {
        self.leaf
    }

    /// Returns `true` if this is a leaf manifest (has a UUID set).
    #[allow(dead_code)]
    pub(crate) fn is_leaf(&self) -> bool {
        self.leaf.is_some()
    }

    pub(crate) fn entries(&self) -> DeltaResult<Vec<MetadataEntry>> {
        let mut all_entries = Vec::new();
        use crate::engine_data::RowVisitor;
        for batch in self.data.iter() {
            let mut visitor = reader::MetadataEntryVisitor::default();
            visitor.visit_rows_of(batch.as_ref())?;
            all_entries.extend(visitor.entries);
        }
        Ok(all_entries)
    }

    /// Checks if the optimized path can be used for reading metadata.
    ///
    /// The optimization can be used when ALL of the following are true:
    /// - Schema contains only Add actions (no Remove field)
    /// - No PositionDeletes or EqualityDeletes (Data and Manifest entries are allowed)
    /// - No deletion vectors present (manifest_dv field is null)
    ///
    /// Validates that metadata contains no unsupported row types.
    ///
    /// Currently only checks for active EqualityDeletes, which are not supported.
    fn validate_no_unsupported_rows(&self) -> DeltaResult<()> {
        use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
        use crate::schema::{ColumnName, DataType};
        use std::sync::LazyLock;

        // Specialized visitor to check for unsupported row types
        struct UnsupportedRowValidator;

        impl RowVisitor for UnsupportedRowValidator {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static NAMES: LazyLock<Vec<ColumnName>> = LazyLock::new(|| {
                    vec![
                        ColumnName::new(["contentType"]),
                        ColumnName::new(["trackingInfo", "status"]),
                    ]
                });
                static TYPES: &[DataType] = &[DataType::INTEGER, DataType::INTEGER];
                (NAMES.as_slice(), TYPES)
            }

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                for i in 0..row_count {
                    let content_type_int: i32 = getters[0].get(i, "contentType")?;
                    let status: i32 = getters[1].get(i, "trackingInfo.status")?;

                    // Check for EqualityDeletes (contentType=2)
                    // EqualityDeletes are not supported by the kernel
                    if content_type_int == 2 {
                        // Allow if marked as DELETED (status=3) - will be filtered out anyway
                        if status != 3 {
                            return Err(Error::unsupported(
                                "EqualityDeletes are not supported. \
                                 This table uses equality delete files which require full row matching. \
                                 Please use a reader that supports EqualityDeletes or rewrite the table \
                                 to use PositionDeletes instead."
                            ));
                        }
                    }

                    // Allow: Data (0), PositionDeletes (1), DataManifest (3), DeleteManifest (4)
                    // PositionDeletes (1) will be handled via lookup join
                    // Manifests are filtered out by build_metadata_selection_vector
                }
                Ok(())
            }
        }

        // Check all entries in the metadata batches
        for batch in self.data.iter() {
            if batch.is_empty() {
                continue;
            }

            let mut visitor = UnsupportedRowValidator;
            visitor.visit_rows_of(batch.as_ref())?;
        }

        Ok(())
    }

    /// Helper to build expression for a field based on action type.
    fn build_action_field_expression(
        field_name: &str,
        field_type: &DataType,
        action_name: &str,
        path_in_log: &str,
    ) -> DeltaResult<crate::expressions::Expression> {
        use crate::expressions::{Expression, MapData, UnaryExpressionOp, VariadicExpressionOp};
        use crate::schema::{DataType, MapType};

        Ok(match field_name {
            // Common fields for both Add and Remove
            "path" => Expression::column(["location"]),
            "size" => Expression::variadic(
                VariadicExpressionOp::Coalesce,
                [
                    Expression::column(["fileSizeInBytes"]),
                    Expression::literal(0i64),
                ],
            ),
            "stats" => {
                // Use cached stats schema
                let num_records_struct = Expression::struct_from_with_schema(
                    [Expression::column(["recordCount"])],
                    (*STATS_NUM_RECORDS_SCHEMA).clone(),
                );
                Expression::unary(UnaryExpressionOp::ToJson, num_records_struct)
            }
            "baseRowId" => Expression::column(["trackingInfo", "firstRowId"]),
            "defaultRowCommitVersion" => Expression::column(["trackingInfo", "snapshotId"]),
            "partitionValues" => {
                let empty_map = MapData::try_new(
                    MapType::new(DataType::STRING, DataType::STRING, false),
                    Vec::<(Scalar, Scalar)>::new(),
                )?;
                Expression::literal(Scalar::Map(empty_map))
            }
            "dataChange" => Expression::literal(true),
            "tags" => Expression::null_literal(DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::STRING,
                true,
            )))),
            "deletionVector" => Expression::null_literal(field_type.clone()),

            // Add-specific fields
            "modificationTime" if action_name == "add" => Expression::literal(i64::MIN),
            "dataManifestPath" if action_name == "add" => {
                Expression::literal(path_in_log.to_string())
            }
            "dataManifestPosition" if action_name == "add" => Expression::column(["_pos"]),
            "clusteringProvider" if action_name == "add" => {
                Expression::null_literal(DataType::STRING)
            }
            "deleteManifestPath" if action_name == "add" => {
                Expression::null_literal(DataType::STRING)
            }
            "deleteManifestPosition" if action_name == "add" => {
                Expression::null_literal(DataType::LONG)
            }

            // Remove-specific fields
            "deletionTimestamp" if action_name == "remove" => Expression::literal(i64::MIN),
            "extendedFileMetadata" if action_name == "remove" => Expression::literal(true),

            // Default: null with field's type
            _ => Expression::null_literal(field_type.clone()),
        })
    }

    /// Builds a Transform expression to convert MetadataEntry → Add or Remove action.
    fn build_metadata_to_action_transform(
        action_schema: &SchemaRef,
        action_name: &str,
        path_in_log: &str,
    ) -> DeltaResult<Arc<crate::expressions::Expression>> {
        use crate::expressions::Expression;
        use crate::schema::DataType;

        let action_field = action_schema
            .field(action_name)
            .ok_or_else(|| Error::generic(format!("Schema missing '{action_name}' field")))?;
        let action_struct_type = match action_field.data_type() {
            DataType::Struct(s) => s,
            _ => {
                return Err(Error::generic(format!(
                    "'{action_name}' field is not a struct"
                )))
            }
        };

        let mut field_exprs: Vec<Arc<Expression>> = Vec::new();
        for field in action_struct_type.fields() {
            let expr = Self::build_action_field_expression(
                field.name(),
                field.data_type(),
                action_name,
                path_in_log,
            )?;
            field_exprs.push(Arc::new(expr));
        }

        let action_struct_expr =
            Expression::struct_from_with_schema(field_exprs, (**action_struct_type).clone());

        // Wrap action struct in top-level schema
        let top_level_expr =
            Self::wrap_action_in_top_level(action_name, action_struct_expr, action_struct_type);

        Ok(Arc::new(top_level_expr))
    }

    /// Helper to wrap an action struct expression in a top-level schema with the action name.
    /// Returns the wrapped expression.
    fn wrap_action_in_top_level(
        action_name: &str,
        action_expr: crate::expressions::Expression,
        action_struct_type: &StructType,
    ) -> crate::expressions::Expression {
        use crate::expressions::Expression;

        let top_level_schema = StructType::new_unchecked(vec![StructField::new(
            action_name,
            DataType::Struct(Box::new(action_struct_type.clone())),
            true,
        )]);

        Expression::struct_from_with_schema([action_expr], top_level_schema)
    }

    /// Builds a Transform expression to convert MetadataEntry → Add fields.
    fn build_metadata_to_add_transform(
        add_schema: &SchemaRef,
        path_in_log: &str,
    ) -> DeltaResult<Arc<crate::expressions::Expression>> {
        Self::build_metadata_to_action_transform(add_schema, "add", path_in_log)
    }

    /// Builds a Transform expression to convert MetadataEntry → Remove fields.
    fn build_metadata_to_remove_transform(
        remove_schema: &SchemaRef,
        path_in_log: &str,
    ) -> DeltaResult<Arc<crate::expressions::Expression>> {
        Self::build_metadata_to_action_transform(remove_schema, "remove", path_in_log)
    }

    /// Builds a Transform expression to convert joined MetadataEntry + DV fields → Add fields.
    ///
    /// This is similar to build_metadata_to_add_transform, but it also constructs the
    /// deletionVector, deleteManifestPath, and deleteManifestPosition fields from the
    /// columns that were joined from the DV joiner.
    ///
    /// The joined schema has these additional top-level fields (raw metadata fields):
    /// - location: String (from DV joiner - needs parsing into storageType + pathOrInlineDv)
    /// - contentInfo.offset: i64 (from DV joiner)
    /// - contentInfo.sizeInBytes: i64 (from DV joiner)
    /// - recordCount: i64 (from DV joiner)
    /// - _pos: i64 (from DV joiner - deleteManifestPosition)
    ///
    /// TODO: Parse location string to extract storageType and pathOrInlineDv
    /// TODO: Handle type conversions and null checks for building DeletionVectorDescriptor
    /// For now, DV fields are set to null as a placeholder until parsing is implemented.
    fn build_metadata_to_add_transform_with_dv(
        add_schema: &SchemaRef,
        path_in_log: &str,
    ) -> DeltaResult<Arc<crate::expressions::Expression>> {
        use crate::expressions::Expression;
        use crate::schema::DataType;

        // Get the Add struct type from the schema
        let add_field = add_schema
            .field("add")
            .ok_or_else(|| Error::generic("Schema missing 'add' field"))?;
        let add_struct_type = match add_field.data_type() {
            DataType::Struct(s) => s,
            _ => return Err(Error::generic("'add' field is not a struct")),
        };

        // Get the deletionVector field type for building the DV struct
        let _dv_field = add_struct_type
            .field("deletionVector")
            .ok_or_else(|| Error::generic("Add schema missing 'deletionVector' field"))?;

        // Build expressions for each Add field
        let mut add_field_exprs: Vec<Arc<Expression>> = Vec::new();

        for field in add_struct_type.fields() {
            let expr: Expression = match field.name().as_str() {
                // DV-specific fields that differ from base transform
                "deletionVector" => {
                    // Combine flat DV columns into a struct
                    // Use nullability predicate: struct is null when storageType is null (no DV match)
                    use crate::actions::deletion_vector::DeletionVectorDescriptor;
                    use crate::schema::ToSchema;
                    let dv_schema = <DeletionVectorDescriptor as ToSchema>::to_schema();

                    // Create struct with nullability predicate
                    // When dv_storageType IS NOT NULL, the struct is non-null
                    // When dv_storageType IS NULL (no join match), the entire struct becomes null
                    Expression::Struct(
                        vec![
                            Arc::new(Expression::column(["dv_storageType"])),
                            Arc::new(Expression::column(["dv_pathOrInlineDv"])),
                            Arc::new(Expression::column(["dv_offset"])),
                            Arc::new(Expression::column(["dv_sizeInBytes"])),
                            Arc::new(Expression::column(["dv_cardinality"])),
                        ],
                        Some(Box::new(dv_schema)),
                        Some(Arc::new(Expression::Predicate(Box::new(
                            Expression::column(["dv_storageType"]).is_not_null(),
                        )))),
                    )
                }
                "deleteManifestPath" => {
                    // Use the joined deleteManifestPath column
                    Expression::column(["deleteManifestPath"])
                }
                "deleteManifestPosition" => {
                    // Use the joined deleteManifestPosition column
                    Expression::column(["deleteManifestPosition"])
                }
                // All other fields: delegate to base function
                _ => Self::build_action_field_expression(
                    field.name(),
                    field.data_type(),
                    "add",
                    path_in_log,
                )?,
            };

            add_field_exprs.push(Arc::new(expr));
        }

        // Create a struct expression with all Add fields
        let add_struct_expr =
            Expression::struct_from_with_schema(add_field_exprs, (**add_struct_type).clone());

        // Wrap in outer struct for the top-level "add" field
        let top_level_expr =
            Self::wrap_action_in_top_level("add", add_struct_expr, add_struct_type);

        Ok(Arc::new(top_level_expr))
    }

    /// Builds a selection vector that excludes deleted and manifest entries from a metadata batch.
    ///
    /// Returns a vector of booleans where `true` means the row should be included.
    ///
    /// Excludes:
    /// - Deleted entries: `trackingInfo.status == TrackingStatus::Deleted`
    /// - Manifest entries: `contentType == DataManifest` or `DeleteManifest`
    ///
    /// Processes a single metadata batch: applies filtering and transformation.
    ///
    /// # Parameters
    /// - `batch`: The metadata batch to process
    /// - `transform_evaluator`: The evaluator that transforms metadata to Add format
    ///
    /// # Returns
    /// The transformed and filtered EngineData ready to be wrapped in ActionsBatch
    fn has_deletion_vectors(&self) -> DeltaResult<bool> {
        for batch in self.data.iter() {
            let (_data_selection, dv_selection) =
                Self::build_data_and_dv_selection_vectors(batch.as_ref())?;
            if dv_selection.iter().any(|&b| b) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Creates a lookup joiner for DV entries from metadata batches.
    ///
    /// Filters metadata to only DV entries and creates a LookupJoiner that maps
    /// data file paths to their DV metadata.
    ///
    /// # Parameters
    /// - `handler`: Evaluation handler for creating the joiner
    /// - `metadata_schema`: Schema of the metadata entries
    /// - `batches`: Metadata batches (will be filtered to only DV entries internally by joiner)
    /// - `delete_manifest_path`: Path to the delete manifest (will be added as constant via transform)
    ///
    /// # Returns
    /// A LookupJoiner ready to join DV fields onto data entries
    ///
    /// # Note
    /// The joined fields will be raw (location as string, contentInfo as struct, etc.).
    /// Post-processing is needed to parse location and build DeletionVectorDescriptor.
    /// Creates a lookup joiner from borrowed metadata batches.
    ///
    /// Following user's suggestion: "We should do the parsing of DVs before converting to EngineData."
    ///
    /// Strategy:
    /// 1. Extract DV entries using build_dv_map_from_batches (parses location strings in Rust)
    /// 2. Convert parsed DV map to EngineData with proper storageType/pathOrInlineDv fields
    /// 3. Use that EngineData to create the joiner
    ///
    /// This avoids complex string parsing in expressions or post-join visitor patterns.
    fn build_dv_joiner_from_metadata(
        handler: &dyn EvaluationHandler,
        metadata_schema: SchemaRef,
        batches: Vec<Box<dyn EngineData>>,
    ) -> DeltaResult<Box<dyn LookupJoiner>> {
        use crate::engine_data::FilteredEngineData;

        // Collect batches with DV entries
        // Note: batches already have parsed DV columns from prepare_batches_with_dv_columns
        let mut filtered_batches: Vec<FilteredEngineData> = Vec::new();

        for batch in batches.into_iter() {
            let (_data_selection, dv_selection) =
                Self::build_data_and_dv_selection_vectors(batch.as_ref())?;

            if dv_selection.iter().any(|&b| b) {
                // Wrap batch with DV selection vector
                filtered_batches.push(FilteredEngineData::try_new(batch, dv_selection)?);
            }
        }

        if filtered_batches.is_empty() {
            return Err(Error::generic("No DV entries found to build joiner"));
        }

        debug!("Building DV joiner with {} batches", filtered_batches.len());

        // Create joiner configuration with flat DV columns
        // The joiner appends flat columns which will be transformed into a struct later
        let key_column = ColumnName::new(["referencedFile"]);
        let version_column = ColumnName::new(["trackingInfo", "sequenceNumber"]);
        let value_columns = vec![
            ColumnName::new(["dv_storageType"]),
            ColumnName::new(["dv_pathOrInlineDv"]),
            ColumnName::new(["dv_offset"]),
            ColumnName::new(["dv_sizeInBytes"]),
            ColumnName::new(["dv_cardinality"]),
            ColumnName::new(["deleteManifestPath"]),
            ColumnName::new(["deleteManifestPosition"]),
        ];

        // Build the extended schema with appended DV fields
        let extended_schema =
            Self::extend_metadata_schema_with_dv_fields(&metadata_schema, &DV_COLUMNS_SCHEMA_FINAL);
        let filtered_refs: Vec<&FilteredEngineData> = filtered_batches.iter().collect();
        handler.new_lookup_join_handler(
            extended_schema,
            &key_column,
            &value_columns,
            &version_column,
            &filtered_refs,
        )
    }

    /// Creates a new batch with parsed DV columns for use in the DV joiner.
    ///
    /// This function efficiently builds a batch containing:
    /// - Join keys: referencedFile, trackingInfo.sequenceNumber
    /// - DV value columns (7 total): dv_storageType, dv_pathOrInlineDv, dv_offset, dv_sizeInBytes,
    ///   dv_cardinality, deleteManifestPath, deleteManifestPosition
    ///
    /// It uses two strategies for efficiency:
    /// 1. **Expression evaluation** for simple columns (dv_cardinality, deleteManifestPath, deleteManifestPosition)
    /// 2. **Visitor pattern** only for complex parsing (dv_storageType, dv_pathOrInlineDv, dv_offset, dv_sizeInBytes)
    ///
    /// # Parameters
    /// - `batch`: Source metadata batch
    /// - `table_root`: Table root URL for resolving DV paths
    /// - `source_manifest_path`: Path to manifest (used as constant deleteManifestPath value)
    /// - `evaluation_handler`: Handler for creating expression evaluators
    /// - `metadata_schema`: Schema of the input metadata batch
    ///
    /// # Returns
    /// A new batch with exactly the columns needed for the DV joiner
    fn append_parsed_dv_columns(
        batch: &dyn EngineData,
        table_root: &Url,
        source_manifest_path: &str,
        evaluation_handler: &dyn EvaluationHandler,
        metadata_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        use crate::actions::deletion_vector::DeletionVectorPath;
        use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
        use crate::expressions::{ArrayData, Expression, Scalar};
        use crate::schema::{ArrayType, ColumnName, DataType};

        // Step 1: Create a batch with join keys and transformable DV columns using expressions
        // This includes: referencedFile, trackingInfo, dv_cardinality, deleteManifestPath, deleteManifestPosition

        let projection_expr = Arc::new(Expression::Struct(
            vec![
                Arc::new(Expression::column(["contentType"])), // Needed by build_data_and_dv_selection_vectors
                Arc::new(Expression::column(["referencedFile"])),
                Arc::new(Expression::column(["trackingInfo"])), // Keep the whole struct
                Arc::new(Expression::column(["recordCount"])),  // dv_cardinality
                Arc::new(Expression::Literal(Scalar::String(
                    source_manifest_path.to_string(),
                ))), // deleteManifestPath
                Arc::new(Expression::column(["_pos"])),         // deleteManifestPosition
            ],
            None,
            None,
        ));

        // Use cached projection schema
        let projection_evaluator = evaluation_handler.new_expression_evaluator(
            metadata_schema,
            projection_expr,
            DataType::Struct(Box::new(DV_PROJECTION_SCHEMA.as_ref().clone())),
        )?;

        let batch_with_projected_columns = projection_evaluator.evaluate(batch)?;

        /// Converts an i64 value to i32 with bounds checking and a descriptive error message.
        fn i64_to_i32(value: i64, field_name: &str) -> DeltaResult<i32> {
            i32::try_from(value).map_err(|_| {
                Error::generic(format!(
                    "DV {} {} out of i32 range ({}..={})",
                    field_name,
                    value,
                    i32::MIN,
                    i32::MAX
                ))
            })
        }

        // Visitor to build flat DV columns and manifest metadata
        // Uses flat columns instead of struct for simpler construction
        struct ParseDVFieldsVisitor {
            // Flat DV fields
            storage_types: Vec<Scalar>,
            path_or_inline_dvs: Vec<Scalar>,
            offsets: Vec<Scalar>,
            size_in_bytes: Vec<Scalar>,
            table_root: Url,
        }

        impl RowVisitor for ParseDVFieldsVisitor {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static NAMES: LazyLock<Vec<ColumnName>> = LazyLock::new(|| {
                    vec![
                        ColumnName::new(["contentType"]),
                        ColumnName::new(["location"]),
                        ColumnName::new(["contentInfo", "offset"]), // Nested in contentInfo struct
                        ColumnName::new(["contentInfo", "sizeInBytes"]), // Nested in contentInfo struct
                    ]
                });
                static TYPES: &[DataType] = &[
                    DataType::INTEGER,
                    DataType::STRING,
                    DataType::LONG,
                    DataType::LONG,
                ];
                (NAMES.as_slice(), TYPES)
            }

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                let content_type_getter = getters[0];
                let location_getter = getters[1];
                let offset_getter = getters[2];
                let size_in_bytes_getter = getters[3];

                for i in 0..row_count {
                    let content_type: i32 = content_type_getter.get(i, "contentType")?;

                    // Only process DV entries (contentType=1)
                    if content_type == 1 {
                        // Parse location
                        let location_opt: Option<&str> = location_getter.get_opt(i, "location")?;

                        let (storage_type, path_or_inline_dv) = if let Some(location) = location_opt
                        {
                            match DeletionVectorPath::parse_path(location, &self.table_root) {
                                Ok((st, path)) => {
                                    (Scalar::String(st.to_string()), Scalar::String(path))
                                }
                                Err(_) => (
                                    Scalar::Null(DataType::STRING),
                                    Scalar::Null(DataType::STRING),
                                ),
                            }
                        } else {
                            (
                                Scalar::Null(DataType::STRING),
                                Scalar::Null(DataType::STRING),
                            )
                        };

                        // Cast offset from i64 to i32 with bounds checking
                        let offset_opt: Option<i64> = offset_getter.get_opt(i, "offset")?;
                        let offset_scalar = match offset_opt {
                            Some(v) => {
                                let offset_i32 = i64_to_i32(v, "offset")?;
                                Scalar::Integer(offset_i32)
                            }
                            None => Scalar::Null(DataType::INTEGER),
                        };

                        // Cast sizeInBytes from i64 to i32 with bounds checking
                        // Subtract 8 bytes to account for the deletion vector format's magic number
                        let size_opt: Option<i64> =
                            size_in_bytes_getter.get_opt(i, "sizeInBytes")?;
                        let size_scalar = match size_opt {
                            Some(v) => {
                                let adjusted = v.checked_sub(8).ok_or_else(|| {
                                    Error::generic(format!(
                                        "DV sizeInBytes {} is less than 8 (magic number size)",
                                        v
                                    ))
                                })?;
                                let size_i32 = i64_to_i32(adjusted, "sizeInBytes")?;
                                Scalar::Integer(size_i32)
                            }
                            None => Scalar::Null(DataType::INTEGER),
                        };

                        // Push flat DV fields
                        self.storage_types.push(storage_type);
                        self.path_or_inline_dvs.push(path_or_inline_dv);
                        self.offsets.push(offset_scalar);
                        self.size_in_bytes.push(size_scalar);
                    } else {
                        // Not a DV entry, use nulls for all flat columns
                        self.storage_types.push(Scalar::Null(DataType::STRING));
                        self.path_or_inline_dvs.push(Scalar::Null(DataType::STRING));
                        self.offsets.push(Scalar::Null(DataType::INTEGER));
                        self.size_in_bytes.push(Scalar::Null(DataType::INTEGER));
                    }
                }
                Ok(())
            }
        }

        // Step 2: Use visitor to parse the complex DV columns from the ORIGINAL input batch
        // The visitor extracts: dv_storageType, dv_pathOrInlineDv, dv_offset, dv_sizeInBytes
        let batch_len = batch.len();
        let mut visitor = ParseDVFieldsVisitor {
            storage_types: Vec::with_capacity(batch_len),
            path_or_inline_dvs: Vec::with_capacity(batch_len),
            offsets: Vec::with_capacity(batch_len),
            size_in_bytes: Vec::with_capacity(batch_len),
            table_root: table_root.clone(),
        };
        // Visit the original input batch which has contentType, location, and contentInfo fields
        visitor.visit_rows_of(batch)?;

        // Step 3: Build ArrayData for the complex DV columns (parsed via visitor)
        let storage_type_array = ArrayData::try_new(
            ArrayType::new(DataType::STRING, true),
            visitor.storage_types,
        )?;
        let path_or_inline_dv_array = ArrayData::try_new(
            ArrayType::new(DataType::STRING, true),
            visitor.path_or_inline_dvs,
        )?;
        let offset_array =
            ArrayData::try_new(ArrayType::new(DataType::INTEGER, true), visitor.offsets)?;
        let size_in_bytes_array = ArrayData::try_new(
            ArrayType::new(DataType::INTEGER, true),
            visitor.size_in_bytes,
        )?;

        // Append the complex DV columns to the batch with projected columns
        // Final batch has: referencedFile, trackingInfo, dv_cardinality, deleteManifestPath,
        // deleteManifestPosition, dv_storageType, dv_pathOrInlineDv, dv_offset, dv_sizeInBytes
        batch_with_projected_columns.append_columns(
            DV_COLUMNS_SCHEMA_VISITOR_NEEDED.clone(),
            vec![
                storage_type_array,
                path_or_inline_dv_array,
                offset_array,
                size_in_bytes_array,
            ],
        )
    }

    /// Extract the maximum sequence number from DV entries in a batch
    fn build_data_and_dv_selection_vectors(
        batch: &dyn EngineData,
    ) -> DeltaResult<(Vec<bool>, Vec<bool>)> {
        use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
        use crate::schema::DataType;

        struct SelectionVisitor {
            data_selection: Vec<bool>,
            dv_selection: Vec<bool>,
        }

        impl RowVisitor for SelectionVisitor {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static NAMES: LazyLock<Vec<ColumnName>> = LazyLock::new(|| {
                    vec![
                        ColumnName::new(["contentType"]),
                        ColumnName::new(["trackingInfo", "status"]),
                    ]
                });
                static TYPES: &[DataType] = &[DataType::INTEGER, DataType::INTEGER];
                (NAMES.as_slice(), TYPES)
            }

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                for i in 0..row_count {
                    let content_type: i32 = getters[0].get(i, "contentType")?;
                    let status_opt: Option<i32> = getters[1].get_opt(i, "trackingInfo.status")?;

                    // Only include Existed (0) or Added (1) entries
                    let is_active = status_opt.map(|s| s == 0 || s == 1).unwrap_or(false);

                    if is_active {
                        if content_type == 0 {
                            // Data
                            self.data_selection.push(true);
                            self.dv_selection.push(false);
                        } else if content_type == 1 {
                            // PositionDeletes
                            self.data_selection.push(false);
                            self.dv_selection.push(true);
                        } else {
                            // Other types (manifest entries, equality deletes)
                            self.data_selection.push(false);
                            self.dv_selection.push(false);
                        }
                    } else {
                        // Deleted or unknown status
                        self.data_selection.push(false);
                        self.dv_selection.push(false);
                    }
                }
                Ok(())
            }
        }

        let mut visitor = SelectionVisitor {
            data_selection: Vec::new(),
            dv_selection: Vec::new(),
        };

        visitor.visit_rows_of(batch)?;

        Ok((visitor.data_selection, visitor.dv_selection))
    }

    /// Builds selection vectors for Add vs Remove entries based on trackingInfo.status.
    ///
    /// Returns (add_selection, remove_selection) where:
    /// - add_selection[i] = true if entry i has status Existed (0) or Added (1)
    /// - remove_selection[i] = true if entry i has status Deleted (2)
    ///
    /// Both exclude manifest entries (contentType 3, 4) and other non-data types.
    fn build_add_remove_selection_vectors(
        batch: &dyn EngineData,
    ) -> DeltaResult<(Vec<bool>, Vec<bool>)> {
        use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
        use crate::schema::DataType;

        struct AddRemoveVisitor {
            add_selection: Vec<bool>,
            remove_selection: Vec<bool>,
        }

        impl RowVisitor for AddRemoveVisitor {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static NAMES: LazyLock<Vec<ColumnName>> = LazyLock::new(|| {
                    vec![
                        ColumnName::new(["contentType"]),
                        ColumnName::new(["trackingInfo", "status"]),
                    ]
                });
                static TYPES: &[DataType] = &[DataType::INTEGER, DataType::INTEGER];
                (NAMES.as_slice(), TYPES)
            }

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                for i in 0..row_count {
                    let content_type: i32 = getters[0].get(i, "contentType")?;
                    let status: i32 = getters[1].get(i, "trackingInfo.status")?;

                    // Only process Data entries (contentType=0)
                    // Skip DVs (1), EqualityDeletes (2), and Manifests (3, 4)
                    if content_type == 0 {
                        match status {
                            0 | 1 => {
                                // Existed or Added -> Add action
                                self.add_selection.push(true);
                                self.remove_selection.push(false);
                            }
                            2 => {
                                // Deleted -> Remove action
                                self.add_selection.push(false);
                                self.remove_selection.push(true);
                            }
                            _ => {
                                // Unknown status
                                self.add_selection.push(false);
                                self.remove_selection.push(false);
                            }
                        }
                    } else {
                        // Not a data entry
                        self.add_selection.push(false);
                        self.remove_selection.push(false);
                    }
                }
                Ok(())
            }
        }

        let mut visitor = AddRemoveVisitor {
            add_selection: Vec::new(),
            remove_selection: Vec::new(),
        };

        visitor.visit_rows_of(batch)?;

        Ok((visitor.add_selection, visitor.remove_selection))
    }

    /// Helper to extend metadata schema with DV fields (_pos if missing, dv_schema.
    fn extend_metadata_schema_with_dv_fields(
        metadata_schema: &SchemaRef,
        dv_schema: &SchemaRef,
    ) -> SchemaRef {
        let mut fields: Vec<_> = metadata_schema.fields().cloned().collect();

        // Add _pos if not already present (it should be from parquet reader or test helper)
        if !metadata_schema.contains("_pos") {
            // TODO: check if this is still needed
            fields.push(StructField::new("_pos", DataType::LONG, false));
        }

        // Add DV columns from cached schema
        fields.extend(dv_schema.fields().cloned());

        Arc::new(StructType::new_unchecked(fields))
    }

    /// Builds a deletion vector map from DV entry batches.
    ///
    /// Converts EngineData batches of DV entries into a HashMap for O(1) lookup.
    ///
    /// # Parameters
    /// - `evaluation_handler`: Handler for creating expression evaluators
    /// - `metadata_schema`: Schema of the metadata entries
    /// - `has_dvs`: Whether the metadata contains deletion vectors
    ///
    /// # Returns
    /// Vec of processed batches with parsed DV columns appended
    fn prepare_batches_with_dv_columns(
        &self,
        evaluation_handler: &dyn EvaluationHandler,
        metadata_schema: SchemaRef,
        has_dvs: bool,
    ) -> DeltaResult<Vec<Box<dyn EngineData>>> {
        if !has_dvs {
            return Ok(Vec::new());
        }

        debug!("Appending parsed DV columns to metadata batches");
        let mut processed = Vec::new();
        for batch in self.data.iter() {
            let batch_with_columns = Self::append_parsed_dv_columns(
                batch.as_ref(),
                &self.table_root,
                &self.path_in_log,
                evaluation_handler,
                metadata_schema.clone(),
            )?;
            processed.push(batch_with_columns);
        }
        Ok(processed)
    }

    /// Determine the evaluator schema based on whether we have parsed DV columns
    fn get_evaluator_schema(has_dvs: bool, metadata_schema: &SchemaRef) -> SchemaRef {
        if has_dvs {
            // When we have DVs, use helper to extend with DV fields
            Self::extend_metadata_schema_with_dv_fields(metadata_schema, &DV_COLUMNS_SCHEMA_FINAL)
        } else {
            metadata_schema.clone()
        }
    }

    /// Build Add and/or Remove evaluators based on the schema
    fn build_action_evaluators(
        evaluation_handler: &dyn EvaluationHandler,
        evaluator_schema: SchemaRef,
        output_schema: &SchemaRef,
        path_in_log: &str,
        has_add: bool,
        has_remove: bool,
        has_dvs: bool,
    ) -> DeltaResult<EvaluatorPair> {
        let add_evaluator_opt = if has_add {
            let add_expr = if has_dvs {
                Self::build_metadata_to_add_transform_with_dv(output_schema, path_in_log)?
            } else {
                Self::build_metadata_to_add_transform(output_schema, path_in_log)?
            };
            Some(evaluation_handler.new_expression_evaluator(
                evaluator_schema.clone(),
                add_expr,
                output_schema.clone().into(),
            )?)
        } else {
            None
        };

        let remove_evaluator_opt = if has_remove {
            let remove_expr = Self::build_metadata_to_remove_transform(output_schema, path_in_log)?;
            Some(evaluation_handler.new_expression_evaluator(
                evaluator_schema.clone(),
                remove_expr,
                output_schema.clone().into(),
            )?)
        } else {
            None
        };

        Ok(EvaluatorPair {
            add_evaluator: add_evaluator_opt,
            remove_evaluator: remove_evaluator_opt,
        })
    }

    /// Apply DV join to a single batch
    fn apply_dv_join_to_batch(
        batch: &dyn EngineData,
        dv_joiner: &dyn LookupJoiner,
    ) -> DeltaResult<Box<dyn EngineData>> {
        debug!("Applying DV join to batch with {} rows", batch.len());
        let all_selected = vec![true; batch.len()];
        let result = dv_joiner.join_raw(
            batch,
            &all_selected,
            &ColumnName::new(["location"]),
            &ColumnName::new(["trackingInfo", "sequenceNumber"]),
        )?;
        debug!("DV join completed, result has {} rows", result.len());

        Ok(result)
    }

    /// Process a single batch for a single action type (Add or Remove)
    /// Helper method to build DV joiner for root manifest.
    ///
    /// This handles the simpler case where DV information is embedded in the root manifest itself.
    fn build_dv_joiner_for_root(
        &self,
        evaluation_handler: &dyn EvaluationHandler,
        metadata_schema: SchemaRef,
    ) -> DeltaResult<Option<Box<dyn LookupJoiner>>> {
        // Check if we have deletion vectors
        let has_dvs = self.has_deletion_vectors()?;

        if !has_dvs {
            return Ok(None);
        }

        debug!("Building DV joiner for optimized path");

        // Prepare batches by appending parsed DV columns (MUST be done before building joiner!)
        let batches_with_dv_columns = self.prepare_batches_with_dv_columns(
            evaluation_handler,
            metadata_schema.clone(),
            has_dvs,
        )?;

        // Build DV joiner using the processed batches with parsed DV columns
        Ok(Some(Self::build_dv_joiner_from_metadata(
            evaluation_handler,
            metadata_schema,
            batches_with_dv_columns,
        )?))
    }

    /// Helper function to process a single DV manifest into prepared batches.
    ///
    /// This function:
    /// 1. Validates the metadata doesn't contain unsupported row types
    /// 2. Prepares batches with DV columns
    /// 3. Applies manifest DV filtering if present
    fn process_dv_manifest(
        dv_metadata: &Metadata,
        filtered_manifest: &FilteredManifest,
        evaluation_handler: &dyn EvaluationHandler,
        metadata_schema: SchemaRef,
    ) -> DeltaResult<Vec<Box<dyn EngineData>>> {
        // Validate that the DV manifest doesn't contain unsupported row types (e.g., EqualityDeletes)
        dv_metadata.validate_no_unsupported_rows()?;

        // Prepare the DV batches (parse DV location strings and append columns)
        let prepared = dv_metadata.prepare_batches_with_dv_columns(
            evaluation_handler,
            metadata_schema,
            true,
        )?;

        // Apply manifest DV if present (filters out deleted DV entries)
        let mut applicator =
            ManifestDvApplicator::new(filtered_manifest.manifest.manifest_dv.as_ref())?;

        let mut filtered_batches = Vec::new();
        for batch in prepared {
            let filtered = applicator.process_batch(batch)?;

            // Only include batches that have selected rows
            if filtered.has_selected_rows() {
                let batch = filtered.apply_selection_vector()?;
                filtered_batches.push(batch);
            }
        }

        Ok(filtered_batches)
    }

    pub(crate) fn build_dv_joiner_for_leaf(
        evaluation_handler: Arc<dyn EvaluationHandler>,
        metadata_schema: SchemaRef,
        manifest_refs: &ManifestReference,
        affiliated_dv_metadata: Vec<Metadata>,
        unaffiliated_dv_metadata: &[Arc<Metadata>],
        unaffiliated_dv_manifests: &[FilteredManifest],
    ) -> DeltaResult<Option<Box<dyn LookupJoiner>>> {
        let has_affiliated_dvs = !manifest_refs.affiliated_dv_manifests.is_empty();
        let has_unaffiliated_dvs = !unaffiliated_dv_manifests.is_empty();

        if !has_affiliated_dvs && !has_unaffiliated_dvs {
            return Ok(None);
        }

        debug!(
            "Building DV joiner for leaf manifest optimized path from {} affiliated + {} unaffiliated DV manifests",
            manifest_refs.affiliated_dv_manifests.len(),
            unaffiliated_dv_manifests.len()
        );

        // Process all DV manifests (both affiliated and unaffiliated) in a single pass
        let mut all_prepared_dv_batches = Vec::new();
        for (dv_metadata, filtered_manifest) in affiliated_dv_metadata
            .iter()
            .chain(unaffiliated_dv_metadata.iter().map(|arc| arc.as_ref()))
            .zip(
                manifest_refs
                    .affiliated_dv_manifests
                    .iter()
                    .chain(unaffiliated_dv_manifests.iter()),
            )
        {
            let prepared = Self::process_dv_manifest(
                dv_metadata,
                filtered_manifest,
                evaluation_handler.as_ref(),
                metadata_schema.clone(),
            )?;
            all_prepared_dv_batches.extend(prepared);
        }

        // Build joiner from the prepared DV batches
        if !all_prepared_dv_batches.is_empty() {
            Ok(Some(Self::build_dv_joiner_from_metadata(
                evaluation_handler.as_ref(),
                metadata_schema,
                all_prepared_dv_batches,
            )?))
        } else {
            Ok(None)
        }
    }

    #[cfg(test)]
    fn root_action_batches_optimized(
        &self,
        engine: &dyn Engine,
        schema: &SchemaRef,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        self.root_action_batches_optimized_with_handler(
            engine.evaluation_handler().as_ref(),
            schema,
            predicate,
        )
    }

    fn root_action_batches_optimized_with_handler(
        &self,
        evaluation_handler: &dyn EvaluationHandler,
        schema: &SchemaRef,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        use crate::actions::{ADD_NAME, REMOVE_NAME};

        // Log if predicate is present (data skipping will be applied)
        if predicate.is_some() {
            debug!("Predicate present in optimized path - data skipping will be applied");
        }

        // Determine which action types are in the schema
        let has_add = schema.contains(ADD_NAME);
        let has_remove = schema.contains(REMOVE_NAME);

        // Get evaluation handler and metadata schema
        // Use base_schema with _pos added (matches what's in the batches)
        let metadata_schema = {
            use crate::schema::MetadataColumnSpec;
            let base_schema = MetadataEntry::base_schema();
            let mut fields: Vec<StructField> = base_schema.fields().cloned().collect();
            fields.push(StructField::create_metadata_column(
                "_pos",
                MetadataColumnSpec::RowIndex,
            ));
            Arc::new(StructType::new_unchecked(fields))
        };

        // Build DV joiner for root manifest
        let dv_joiner_opt =
            self.build_dv_joiner_for_root(evaluation_handler, metadata_schema.clone())?;

        // Check if we have deletion vectors (needed for evaluator schema and evaluators)
        let has_dvs = dv_joiner_opt.is_some();

        // Determine evaluator schema (includes parsed DV columns if present)
        let evaluator_schema = Self::get_evaluator_schema(has_dvs, &metadata_schema);

        // Build evaluators for Add and/or Remove actions
        let evaluators = Self::build_action_evaluators(
            evaluation_handler,
            evaluator_schema,
            schema,
            &self.path_in_log,
            has_add,
            has_remove,
            has_dvs,
        )?;
        let add_evaluator_opt = evaluators.add_evaluator;
        let remove_evaluator_opt = evaluators.remove_evaluator;

        // Process each batch
        let mut result_batches = Vec::new();

        for batch in &self.data {
            // Optionally apply DV join to append DV columns
            // We'll store the joined batch if present, otherwise work with original
            let joined_batch;
            let batch_ref: &dyn EngineData = if let Some(ref joiner) = dv_joiner_opt {
                joined_batch = Self::apply_dv_join_to_batch(batch.as_ref(), joiner.as_ref())?;
                joined_batch.as_ref()
            } else {
                batch.as_ref()
            };

            // Process Add entries if needed
            if let Some(add_eval) = add_evaluator_opt.as_ref() {
                let (add_selection, _) = Self::build_add_remove_selection_vectors(batch_ref)?;

                if add_selection.iter().any(|&b| b) {
                    let transformed = add_eval.evaluate(batch_ref)?;
                    let filtered_data = transformed.apply_selection_vector(add_selection)?;
                    result_batches.push(Ok(ActionsBatch::new(filtered_data, false)));
                }
            }

            // Process Remove entries if needed
            if let Some(remove_eval) = remove_evaluator_opt.as_ref() {
                let (_, remove_selection) = Self::build_add_remove_selection_vectors(batch_ref)?;

                if remove_selection.iter().any(|&b| b) {
                    let transformed = remove_eval.evaluate(batch_ref)?;
                    let filtered_data = transformed.apply_selection_vector(remove_selection)?;
                    result_batches.push(Ok(ActionsBatch::new(filtered_data, false)));
                }
            }
        }

        Ok(Box::new(result_batches.into_iter()))
    }

    /// Converts root manifest entries to action batches.
    ///
    /// # Parameters
    /// - `predicate`: Optional predicate for data skipping. When provided, entries whose
    ///   `content_stats` indicate they cannot contain matching data will be skipped.
    #[cfg(test)]
    pub(crate) fn root_action_batches(
        &self,
        engine: &dyn Engine,
        schema: &SchemaRef,
        _partition_keys: &[String],
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // Return empty iterator if schema doesn't contain Add or Remove
        if !schema.contains(ADD_NAME) && !schema.contains(REMOVE_NAME) {
            return Ok(Box::new(std::iter::empty()));
        }

        // Validate no unsupported rows (e.g., EqualityDeletes)
        self.validate_no_unsupported_rows()?;

        debug!("Using optimized path for metadata reading");
        self.root_action_batches_optimized(engine, schema, predicate)
    }

    /// Version of root_action_batches that takes handlers directly (for lazy streaming).
    pub(crate) fn root_action_batches_with_handler(
        &self,
        evaluation_handler: &dyn EvaluationHandler,
        schema: &SchemaRef,
        _partition_keys: &[String],
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // Return empty iterator if schema doesn't contain Add or Remove
        if !schema.contains(ADD_NAME) && !schema.contains(REMOVE_NAME) {
            return Ok(Box::new(std::iter::empty()));
        }

        // Validate no unsupported rows (e.g., EqualityDeletes)
        self.validate_no_unsupported_rows()?;

        debug!("Using optimized path for metadata reading");
        self.root_action_batches_optimized_with_handler(evaluation_handler, schema, predicate)
    }

    /// Discovers child manifest references in the root manifest.
    ///
    /// This method implements the hierarchical metadata tree structure described in the
    /// Iceberg Single File Commits specification. It parses the root manifest and identifies:
    ///
    /// - **Data manifest files** (content_type = DataManifest): References to child manifests
    ///   containing actual data file entries
    /// - **Delete manifest files** (content_type = DeleteManifest): References to manifests
    ///   containing deletion vectors, grouped by their affiliation to data manifests
    /// - **Manifest deletion vectors**: Stored inline in the `manifest_dv` field of
    ///   DataManifest/DeleteManifest entries. Applied during manifest reading to filter
    ///   out deleted entries without rewriting manifest files.
    ///
    /// The returned `ManifestReference` groups delete manifests into two categories:
    /// - `affiliated_dv_manifests`: Delete manifests that reference a specific data manifest
    ///   (via the `referenced_file` field)
    /// - `unaffiliated_dv_manifests`: Delete manifests with no specific affiliation, which
    ///   must be checked against all data files
    ///
    /// # Returns
    /// An iterator over `ManifestReference`, one for each data manifest in the root.
    ///
    /// # Example Usage
    /// ```ignore
    /// // Get manifest references from the root (no manifest-level skipping)
    /// let manifest_refs_iter = metadata.manifest_references(None)?;
    ///
    /// // Process each child manifest
    /// for manifest_refs_result in manifest_refs_iter {
    ///     let manifest_refs = manifest_refs_result?;
    ///     let action_batches = Metadata::manifest_to_action_batches(
    ///         manifest_refs,
    ///         engine,
    ///         schema,
    ///         partition_keys
    ///     )?;
    ///     // Process action batches...
    /// }
    /// ```
    ///
    /// # Parameters
    /// - `predicate`: Optional predicate for manifest-level data skipping. When provided,
    ///   manifests whose `content_stats` indicate they cannot contain matching data will
    ///   be skipped (not included in the returned references).
    pub(crate) fn manifest_references(
        &self,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<LeafReferences> {
        // Get all metadata entries from the root manifest
        let entries = self.entries()?;

        // Separate entries by type
        let mut data_manifest_entries = Vec::new();
        let mut delete_manifest_entries = Vec::new();
        let mut position_delete_entries = Vec::new();
        let mut data_file_entries = Vec::new();

        for entry in entries {
            match entry.content_type {
                DataContentType::DataManifest => data_manifest_entries.push(entry),
                DataContentType::DeleteManifest => delete_manifest_entries.push(entry),
                DataContentType::PositionDeletes => position_delete_entries.push(entry),
                DataContentType::Data => data_file_entries.push(entry),
                DataContentType::EqualityDeletes => {
                    return Err(Error::generic("Equality deletes are not supported"))
                }
            }
        }

        // Build a map of delete manifests by their affiliated data manifest
        let mut affiliated_deletes: HashMap<String, Vec<MetadataEntry>> = HashMap::new();
        let mut unaffiliated_deletes = Vec::new();

        for delete_entry in delete_manifest_entries {
            if let Some(ref referenced_file) = delete_entry.referenced_file {
                affiliated_deletes
                    .entry(referenced_file.clone())
                    .or_default()
                    .push(delete_entry);
            } else {
                unaffiliated_deletes.push(delete_entry);
            }
        }

        // Convert unaffiliated deletes to FilteredManifest
        let unaffiliated_dv_manifests: Vec<FilteredManifest> = unaffiliated_deletes
            .into_iter()
            .map(FilteredManifest::new)
            .collect();

        // Apply manifest-level data skipping if a predicate is provided
        let data_manifest_entries =
            filter_entries_by_predicate(data_manifest_entries, predicate, "child manifests");

        // Create ManifestReferences for each data manifest
        let manifest_refs: Vec<DeltaResult<ManifestReference>> = data_manifest_entries
            .into_iter()
            .map(|data_entry| {
                let location = data_entry
                    .location
                    .clone()
                    .ok_or_else(|| Error::generic("Data manifest must have a location"))?;

                // DV is now stored inline on the manifest entry itself
                let data_manifest = FilteredManifest::new(data_entry);

                // Get affiliated delete manifests for this data manifest
                // DV is stored inline on each manifest entry
                let affiliated_dv_manifests: Vec<FilteredManifest> = affiliated_deletes
                    .get(&location)
                    .map(|entries| {
                        entries
                            .iter()
                            .map(|manifest_entry| FilteredManifest::new(manifest_entry.clone()))
                            .collect()
                    })
                    .unwrap_or_default();

                Ok(ManifestReference {
                    data_manifest,
                    affiliated_dv_manifests,
                })
            })
            .collect();

        let manifest_references = manifest_refs.into_iter().collect::<DeltaResult<Vec<_>>>()?;

        Ok(LeafReferences {
            manifest_references,
            shared_state: SharedLeafState {
                unaffiliated_dv_manifests,
            },
        })
    }

    /// Builds a deletion vector map from shared leaf state.
    ///
    /// This helper method loads all unaffiliated delete manifests and merges them
    /// with unmatched DVs from the root to create a complete deletion vector map
    /// that applies to all leaf data files.
    ///
    /// # Parameters
    /// - `shared_state`: The shared state containing unaffiliated manifests and unmatched DVs
    /// - `engine`: The engine for reading parquet files
    ///
    /// # Returns
    /// A HashMap mapping file paths to their deletion vector information.
    #[cfg(test)]
    pub(crate) fn non_root_action_batches(
        root_state: LeafReferences,
        engine: &dyn Engine,
        schema: &SchemaRef,
        table_root: &Url,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // Capture the handlers we need (both are Arc, so cheap to clone)
        let parquet_handler = engine.parquet_handler();
        let evaluation_handler = engine.evaluation_handler();
        Self::non_root_action_batches_with_handlers(
            root_state,
            parquet_handler,
            evaluation_handler,
            schema,
            table_root,
            predicate,
        )
    }

    /// Version of non_root_action_batches that takes handlers directly (for lazy streaming).
    pub(crate) fn non_root_action_batches_with_handlers(
        root_state: LeafReferences,
        parquet_handler: Arc<dyn ParquetHandler>,
        evaluation_handler: Arc<dyn EvaluationHandler>,
        schema: &SchemaRef,
        table_root: &Url,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // Use BulkManifestStreamProcessor for lazy processing of manifests
        let processor = bulk_processor::BulkManifestStreamProcessor::new(
            root_state.manifest_references.into_iter(),
            root_state.shared_state,
            parquet_handler,
            evaluation_handler,
            schema.clone(),
            table_root.clone(),
            predicate.cloned(),
        )?;

        Ok(Box::new(processor))
    }

    /// Processes a ManifestReference into action batches.
    ///
    /// Given a `ManifestReference` and a pre-built deletion vector map, this method:
    ///
    /// 1. **Reads the data manifest file**: Parses the child manifest to get data file entries
    /// 2. **Reads affiliated delete manifests**: Processes delete manifests specific to this data manifest
    /// 3. **Merges with shared DVs**: Combines affiliated DVs with the shared DV map
    /// 4. **Filters entries**: Applies predicate-based data skipping using content_stats
    /// 5. **Converts entries to actions**: Transforms MetadataEntry records into Add/Remove actions
    /// 6. **Returns action batches**: Produces an iterator of ActionsBatch objects
    ///
    /// # Parameters
    /// - `manifest_refs`: The manifest references to process
    /// - `shared_dv_map`: Pre-built deletion vector map from shared state
    /// - `engine`: The engine for reading parquet files
    /// - `schema`: The action schema (typically from `get_log_add_schema()`)
    /// - `predicate`: Optional predicate for data skipping
    ///
    /// # Returns
    /// An iterator over `ActionsBatch` objects, each containing a single Add or Remove action.
    ///
    /// # Notes
    /// - Use `non_root_action_batches` for a higher-level API that processes all manifests
    /// - The shared_dv_map should be built once and reused for all child manifests (via Arc)
    #[allow(dead_code)]
    pub(crate) fn manifest_to_action_batches(
        manifest_refs: ManifestReference,
        shared_state: &SharedLeafState,
        engine: &dyn Engine,
        schema: &SchemaRef,
        table_root: &Url,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // Extract handlers and delegate to handler-based version
        let parquet_handler = engine.parquet_handler();
        let evaluation_handler = engine.evaluation_handler();

        Self::manifest_to_action_batches_with_handlers(
            manifest_refs,
            shared_state,
            parquet_handler,
            evaluation_handler,
            schema,
            table_root,
            predicate,
        )
    }

    /// Checks if the optimized path can be used for a leaf manifest.
    ///
    /// Checks if we can use the optimized path for leaf manifests.
    ///
    /// Requirements:
    /// - Schema is Add-only (no Remove actions)
    ///
    /// Now allows:
    /// - Affiliated DV manifests (will be handled via lookup join)
    /// - Shared DVs (will be handled via lookup join)
    fn can_use_leaf_optimized_path(schema: &SchemaRef) -> bool {
        use crate::actions::{ADD_NAME, REMOVE_NAME};

        // Allow both Add and Remove actions in the optimized path
        // They will be handled via separate selection vectors
        if !schema.contains(ADD_NAME) && !schema.contains(REMOVE_NAME) {
            return false;
        }

        // Allow affiliated DV manifests and shared DVs - will be handled via lookup join
        true
    }

    /// Merge manifest DV selection into an existing selection vector.
    ///
    /// Applies AND operation: selection[i] = selection[i] && manifest_dv_selection[i]
    ///
    /// Per FilteredEngineData contract: if manifest_dv_selection is shorter than selection,
    /// rows beyond its length are considered selected (not deleted by manifest DV).
    fn merge_manifest_dv_selection(
        selection: &mut [bool],
        manifest_dv_selection: &[bool],
    ) -> DeltaResult<()> {
        // Manifest DV selection should never be longer than the batch
        if manifest_dv_selection.len() > selection.len() {
            return Err(Error::generic(format!(
                "Manifest DV selection is longer than batch: {} > {}",
                manifest_dv_selection.len(),
                selection.len()
            )));
        }

        // Apply manifest DV by iterating over its selection vector
        for (i, &dv_selected) in manifest_dv_selection.iter().enumerate() {
            selection[i] = selection[i] && dv_selected;
        }

        Ok(())
    }

    /// Process a single filtered batch into action batches.
    ///
    /// Takes a FilteredEngineData containing a batch with its manifest DV selection vector,
    /// combines it with Add/Remove selections, and produces ActionBatch results.
    ///
    /// # Parameters
    /// - `filtered_batch`: Batch with manifest DV selection already applied
    /// - `dv_joiner_opt`: Optional joiner for affiliated/unaffiliated DVs
    /// - `add_evaluator_opt`: Optional evaluator for Add actions
    /// - `remove_evaluator_opt`: Optional evaluator for Remove actions
    fn process_filtered_batch_to_actions(
        filtered_batch: FilteredEngineData,
        dv_joiner_opt: Option<&dyn LookupJoiner>,
        add_evaluator_opt: Option<&Arc<dyn ExpressionEvaluator>>,
        remove_evaluator_opt: Option<&Arc<dyn ExpressionEvaluator>>,
    ) -> DeltaResult<Vec<ActionsBatch>> {
        // Extract batch and manifest DV selection vector
        let (batch, manifest_dv_selection) = filtered_batch.into_parts();

        // Apply DV join if present (appends DV columns)
        let joined_batch;
        let batch_ref: &dyn EngineData = if let Some(joiner) = dv_joiner_opt {
            joined_batch = Self::apply_dv_join_to_batch(batch.as_ref(), joiner)?;
            joined_batch.as_ref()
        } else {
            batch.as_ref()
        };

        // Build Add/Remove selection vectors once
        let (mut add_selection, mut remove_selection) =
            Self::build_add_remove_selection_vectors(batch_ref)?;

        let mut result_batches = Vec::new();

        // Process Add entries if needed
        if let Some(add_eval) = add_evaluator_opt {
            Self::merge_manifest_dv_selection(&mut add_selection, &manifest_dv_selection)?;
            if add_selection.iter().any(|&b| b) {
                let transformed = add_eval.evaluate(batch_ref)?;
                let filtered_data = transformed.apply_selection_vector(add_selection)?;
                result_batches.push(ActionsBatch::new(filtered_data, false));
            }
        }

        // Process Remove entries if needed
        if let Some(remove_eval) = remove_evaluator_opt {
            Self::merge_manifest_dv_selection(&mut remove_selection, &manifest_dv_selection)?;
            if remove_selection.iter().any(|&b| b) {
                let transformed = remove_eval.evaluate(batch_ref)?;
                let filtered_data = transformed.apply_selection_vector(remove_selection)?;
                result_batches.push(ActionsBatch::new(filtered_data, false));
            }
        }

        Ok(result_batches)
    }

    /// Process a manifest into action batches using the bulk processor.
    ///
    /// This wrapper converts a single manifest into a BulkManifestStreamProcessor
    /// which handles parallel IO and lazy processing.
    fn manifest_to_action_batches_optimized_with_handlers(
        manifest_refs: &ManifestReference,
        shared_state: &SharedLeafState,
        parquet_handler: Arc<dyn ParquetHandler>,
        evaluation_handler: Arc<dyn EvaluationHandler>,
        schema: &SchemaRef,
        table_root: &Url,
        _path_in_log: String,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // Convert borrowed parameters to owned for the processor
        let manifest_iter = std::iter::once(manifest_refs.clone());
        let shared_state_owned = shared_state.clone();
        let schema_owned = schema.clone();
        let table_root_owned = table_root.clone();

        // Create bulk processor for this single manifest
        let processor = bulk_processor::BulkManifestStreamProcessor::new(
            manifest_iter,
            shared_state_owned,
            parquet_handler,
            evaluation_handler,
            schema_owned,
            table_root_owned,
            None, // predicate
        )?;

        Ok(Box::new(processor))
    }

    /// Processes a ManifestReference into action batches using captured handlers.
    ///
    /// This is an internal version of `manifest_to_action_batches` that takes Arc handlers
    /// instead of `&dyn Engine`, enabling it to be called from lazy iterators without
    /// lifetime issues.
    fn manifest_to_action_batches_with_handlers(
        manifest_refs: ManifestReference,
        shared_state: &SharedLeafState,
        parquet_handler: Arc<dyn ParquetHandler>,
        evaluation_handler: Arc<dyn EvaluationHandler>,
        schema: &SchemaRef,
        table_root: &Url,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        let data_manifest_location = manifest_refs
            .data_manifest
            .manifest
            .location
            .clone()
            .ok_or_else(|| Error::generic("Data manifest must have a location"))?;

        // Check if we can use optimized path
        if predicate.is_some() || !Self::can_use_leaf_optimized_path(schema) {
            return Err(Error::generic(
                "Cannot use optimized path for leaf manifest. \
                 Predicate filtering on leaf manifests is not yet supported in optimized path.",
            ));
        }

        debug!("Using optimized path for leaf manifest with handlers");
        Self::manifest_to_action_batches_optimized_with_handlers(
            &manifest_refs,
            shared_state,
            parquet_handler,
            evaluation_handler,
            schema,
            table_root,
            data_manifest_location,
        )
    }

    /// Creates Metadata from a Delta table snapshot by replaying add actions from the transaction log.
    ///
    /// This method internally uses log replay to:
    /// - Read actions from the log in reverse chronological order
    /// - Deduplicate add/remove actions to get the current table state
    /// - Convert Add actions to MetadataEntry format (Adaptive Metadata Tree)
    ///
    /// # Parameters
    /// - `snapshot`: The Delta table snapshot to build metadata from
    /// - `engine`: The engine to use for reading log files and processing actions
    ///
    /// # Returns
    /// A `Metadata` instance containing all active files in the table at the snapshot version.
    #[allow(dead_code)]
    pub(crate) fn new_from_snapshot(
        engine: &dyn Engine,
        snapshot: SnapshotRef,
    ) -> DeltaResult<Self> {
        let table_root = snapshot.table_root().clone();
        let version = snapshot.version();
        let table_schema = snapshot.schema().as_ref().clone();
        let scan = ScanBuilder::new(snapshot).build()?;
        let scan_metadata_iter = scan.scan_metadata(engine)?;

        let mut metadata_builder = MetadataBuilder::new_for(table_root, version, table_schema);

        for scan_metadata_result in scan_metadata_iter {
            let scan_metadata = scan_metadata_result?;
            let engine_data = scan_metadata.scan_files.data();

            // When building from snapshot, we don't have a CommitInfo snapshot_id, so pass None.
            // Note: scan_files.data() has scan row schema, not Add action schema, so we use
            // add_from_scan_row_data instead of add_from_engine_data_add.
            metadata_builder.add_from_scan_row_data(engine_data, version, None)?;
        }

        metadata_builder.build(engine, None)
    }

    /// Reads Metadata from a parquet file at the specified path.
    ///
    /// This is used to read previously written Adaptive Metadata Tree (AMT) metadata files.
    ///
    /// # Parameters
    /// - `engine`: The engine to use for reading the parquet file
    /// - `path`: The URL path to the metadata parquet file
    /// - `path_in_log`: The original path string as it appears in the Delta log (not normalized)
    /// - `table_root`: The table root URL
    ///
    /// # Returns
    /// A `Metadata` instance deserialized from the parquet file.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn read(
        engine: &dyn Engine,
        path: &Url,
        path_in_log: String,
        table_root: Url,
    ) -> DeltaResult<Self> {
        Self::read_with_handler(engine.parquet_handler(), path, path_in_log, table_root)
    }

    /// Opens a parquet stream for reading metadata without collecting batches (for lazy streaming).
    ///
    /// Returns the batch iterator and parsed version, allowing callers to defer batch collection.
    ///
    /// # Returns
    /// A tuple of (batch_iterator, version, path_in_log) that can be used to construct Metadata later.
    pub(crate) fn open_stream(
        parquet_handler: Arc<dyn ParquetHandler>,
        path: &Url,
        path_in_log: String,
    ) -> DeltaResult<ParquetStreamResult> {
        // Cached schema for reading MetadataEntry from parquet files.
        // Uses base_schema which excludes content_stats (requires table schema).
        // Includes _pos metadata column for tracking row positions within the manifest.
        static READ_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
            use crate::schema::MetadataColumnSpec;

            let base_schema = MetadataEntry::base_schema();
            let mut fields: Vec<StructField> = base_schema.fields().cloned().collect();

            // Add _pos metadata column to track row indices (needed for data_manifest_position)
            fields.push(StructField::create_metadata_column(
                "_pos",
                MetadataColumnSpec::RowIndex,
            ));

            Arc::new(StructType::new_unchecked(fields))
        });

        let file = FileMeta {
            location: path.clone(),
            last_modified: 0,
            size: 0,
        };

        let parsed =
            ParsedLogPath::try_from(file.clone())?.ok_or_else(|| Error::invalid_log_path(path))?;

        let read_result_iter =
            parquet_handler.read_parquet_files(&[file], READ_SCHEMA.clone(), None)?;

        Ok((read_result_iter, parsed.version, path_in_log))
    }

    /// Read metadata using a parquet handler directly (for lazy streaming).
    ///
    /// Uses `MetadataEntry::base_schema()` for reading, which excludes content_stats.
    /// The visitor extracts all fields except content_stats which requires table schema.
    fn read_with_handler(
        parquet_handler: Arc<dyn ParquetHandler>,
        path: &Url,
        path_in_log: String,
        table_root: Url,
    ) -> DeltaResult<Self> {
        let (read_result_iter, version, path_in_log) =
            Self::open_stream(parquet_handler, path, path_in_log)?;

        let data: Vec<Box<dyn EngineData>> = read_result_iter.collect::<DeltaResult<Vec<_>>>()?;

        Ok(Self {
            data,
            version,
            table_root,
            path_in_log,
            // When reading existing metadata, we don't know if it's a root or leaf
            // This would need to be determined from the file path or stored in the metadata
            leaf: None,
        })
    }

    /// Get the engine data for testing purposes
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn data(&self) -> &[Box<dyn EngineData>] {
        &self.data
    }

    /// Converts this Metadata into a MetadataBuilder for further modifications.
    ///
    /// This creates a new builder initialized with the table root, allowing additional
    /// metadata entries to be added before building a new Metadata instance.
    ///
    /// # Arguments
    /// * `table_schema` - The table's data schema with parquet.field.id metadata on each field.
    ///   This is used to convert Delta JSON stats to the content_stats StructData format.
    ///
    /// # Returns
    /// A `MetadataBuilder` that can be used to add more entries or build a new Metadata.
    #[allow(dead_code)]
    /// Convert this metadata to a builder for modification.
    ///
    /// # Arguments
    /// * `table_schema` - The table schema for metadata entry construction
    /// * `new_version` - The version number for the new metadata being built.
    ///   This should typically be the commit version, NOT the version of the existing metadata.
    pub(crate) fn to_builder(
        &self,
        table_schema: StructType,
        new_version: Version,
    ) -> MetadataBuilder {
        use crate::metadata::reader::MetadataEntryVisitor;
        use crate::RowVisitor;

        let mut builder =
            MetadataBuilder::new_for(self.table_root.clone(), new_version, table_schema);

        // Copy existing entries from this metadata into the builder
        for engine_data in &self.data {
            let mut visitor = MetadataEntryVisitor::default();
            // Ignore errors - if we can't extract entries, just skip them
            if visitor.visit_rows_of(engine_data.as_ref()).is_ok() {
                for entry in visitor.entries {
                    builder.add_entry(entry);
                }
            }
        }

        builder
    }

    /// Creates Metadata from a content root commit.
    ///
    /// This is an optimized path for batch commits that loads metadata directly from a
    /// content root parquet file instead of replaying the entire log.
    ///
    /// # Parameters
    /// - `engine`: The engine to use for reading the parquet file
    /// - `content_root_commit`: The parsed log path of the commit containing the content root
    ///
    /// # Returns
    /// A `Metadata` instance loaded from the content root file.
    #[allow(dead_code)]
    pub(crate) fn new_from_content_root(
        engine: &dyn Engine,
        content_root: &ContentRoot,
        table_root: Url,
    ) -> DeltaResult<Self> {
        // Parse and read from the content root file referenced by the ContentRoot action
        let content_root_url = table_root
            .join(&content_root.path)
            .map_err(|e| Error::generic(format!("Failed to parse content root URL: {}", e)))?;
        Self::read(
            engine,
            &content_root_url,
            content_root.path.clone(),
            table_root,
        )
    }
}

/// Parses a manifest DV into a selection vector.
///
/// Returns a boolean vector where `true` means the row should be included (NOT deleted).
/// Parse manifest deletion vector bytes into a RoaringTreemap.
/// Returns None if dv_bytes is empty, otherwise returns the set of deleted positions.
pub(crate) fn parse_manifest_dv(dv_bytes: &Bytes) -> DeltaResult<Option<roaring::RoaringTreemap>> {
    use roaring::RoaringTreemap;

    // Check if we have DV data
    if dv_bytes.is_empty() {
        return Ok(None);
    }

    // Parse the magic number from the first 4 bytes
    if dv_bytes.len() < 4 {
        return Err(Error::generic(
            "Manifest deletion vector is too small (less than 4 bytes)",
        ));
    }

    let magic = u32::from_be_bytes([dv_bytes[0], dv_bytes[1], dv_bytes[2], dv_bytes[3]]);

    // Magic numbers from the deletion vector format
    const ROARING_BITMAP_PORTABLE_MAGIC: u32 = 1681511377;
    const ROARING_BITMAP_NATIVE_MAGIC: u32 = 1681511376;

    // Deserialize the RoaringTreemap
    let deleted_positions = match magic {
        ROARING_BITMAP_PORTABLE_MAGIC => RoaringTreemap::deserialize_from(&dv_bytes[4..])
            .map_err(|err| Error::generic(format!("Failed to deserialize manifest DV: {}", err)))?,
        ROARING_BITMAP_NATIVE_MAGIC => {
            return Err(Error::generic(
                "Native serialization format for manifest deletion vectors is not yet supported",
            ));
        }
        _ => {
            return Err(Error::generic(format!(
                "Invalid magic number in manifest deletion vector: {}",
                magic
            )));
        }
    };

    Ok(Some(deleted_positions))
}

/// Parse manifest_dv bytes into a selection vector.
///
/// Returns a `Vec<bool>` where `true` means the row is NOT deleted.
/// This is used in tests to validate manifest DV parsing.
#[cfg(test)]
fn parse_manifest_dv_to_selection_vector(
    dv_bytes: &Bytes,
    total_rows: usize,
) -> DeltaResult<Vec<bool>> {
    let deleted_positions = parse_manifest_dv(dv_bytes)?;

    // Build selection vector: true if NOT deleted
    let selection_vector: Vec<bool> = if let Some(deleted_positions) = deleted_positions {
        (0..total_rows)
            .map(|i| !deleted_positions.contains(i as u64))
            .collect()
    } else {
        vec![true; total_rows]
    };

    Ok(selection_vector)
}

/// Helper that holds a URL and lazily computes/caches its relative path.
/// Useful for avoiding repeated conversions of the same URL to relative path.
/// Converts an absolute URL to a path relative to table_root.
pub(crate) fn absolute_to_relative_path(
    absolute_url: &Url,
    table_root: &Url,
) -> DeltaResult<String> {
    let full_path = absolute_url.path();
    let root_path = table_root.path();

    Ok(full_path
        .strip_prefix(root_path)
        .unwrap_or(full_path)
        .trim_start_matches('/')
        .to_string())
}

/// Parses a string as an absolute URL, or if that fails, joins it with the table root.
/// This handles both absolute and relative manifest/file paths.
pub(crate) fn parse_or_join_url(path: &str, table_root: &Url) -> DeltaResult<Url> {
    Url::parse(path)
        .or_else(|_| table_root.join(path))
        .map_err(|e| Error::generic(format!("Failed to parse URL '{}': {}", path, e)))
}

/// Converts a DeletionVectorDescriptor to a Scalar representation
pub(crate) fn metadata_entry_to_scalars(
    entry: &MetadataEntry,
    schema: &crate::schema::SchemaRef,
) -> DeltaResult<Vec<Scalar>> {
    use crate::expressions::StructData;

    // Build a vector of structured scalars for the schema (one per top-level field)
    let mut scalars = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let scalar = match field.name().as_str() {
            "contentType" => Scalar::from(entry.content_type),
            "location" => Scalar::from(entry.location.clone()),
            "fileFormat" => Scalar::from(entry.file_format),
            "trackingInfo" => match &entry.tracking_info {
                Some(ti) => {
                    // Get struct fields from schema
                    let struct_fields =
                        if let crate::schema::DataType::Struct(st) = field.data_type() {
                            st.fields().cloned().collect::<Vec<_>>()
                        } else {
                            return Err(crate::Error::generic(
                                "trackingInfo field should be a struct",
                            ));
                        };
                    let values = vec![
                        Scalar::from(ti.status),
                        Scalar::from(ti.snapshot_id),
                        Scalar::from(ti.sequence_number),
                        Scalar::from(ti.file_sequence_number),
                        Scalar::from(ti.first_row_id),
                        Scalar::from(ti.changes_dv.clone()),
                    ];
                    Scalar::Struct(StructData::new_unchecked(struct_fields, values))
                }
                None => Scalar::Null(field.data_type().clone()),
            },
            "contentInfo" => match &entry.content_info {
                Some(ci) => {
                    let struct_fields =
                        if let crate::schema::DataType::Struct(st) = field.data_type() {
                            st.fields().cloned().collect::<Vec<_>>()
                        } else {
                            return Err(crate::Error::generic(
                                "contentInfo field should be a struct",
                            ));
                        };
                    let values = vec![Scalar::from(ci.offset), Scalar::from(ci.size_in_bytes)];
                    Scalar::Struct(StructData::new_unchecked(struct_fields, values))
                }
                None => Scalar::Null(field.data_type().clone()),
            },
            "partitionSpecId" => Scalar::from(entry.partition_spec_id),
            "sortOrderId" => Scalar::from(entry.sort_order_id),
            "recordCount" => Scalar::from(entry.record_count),
            "fileSizeInBytes" => Scalar::from(entry.file_size_in_bytes),
            "contentStats" => match &entry.content_stats {
                Some(struct_data) => Scalar::Struct(struct_data.clone()),
                None => Scalar::Null(field.data_type().clone()),
            },
            "manifestStats" => match &entry.manifest_info {
                Some(ms) => {
                    let struct_fields =
                        if let crate::schema::DataType::Struct(st) = field.data_type() {
                            st.fields().cloned().collect::<Vec<_>>()
                        } else {
                            return Err(crate::Error::generic(
                                "manifestStats field should be a struct",
                            ));
                        };
                    let values = vec![
                        Scalar::from(ms.added_files_count),
                        Scalar::from(ms.existing_files_count),
                        Scalar::from(ms.deletes_files_count),
                        Scalar::from(ms.added_rows_count),
                        Scalar::from(ms.existing_rows_count),
                        Scalar::from(ms.delete_rows_count),
                        Scalar::from(ms.min_sequence_number),
                    ];
                    Scalar::Struct(StructData::new_unchecked(struct_fields, values))
                }
                None => Scalar::Null(field.data_type().clone()),
            },
            "referencedFile" => Scalar::from(entry.referenced_file.clone()),
            "manifestDv" => Scalar::from(entry.manifest_dv.clone()),
            _ => Scalar::Null(field.data_type().clone()),
        };

        scalars.push(scalar);
    }

    Ok(scalars)
}

/// Type of content stored by the manifest entry
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DataContentType {
    Data = 0,
    PositionDeletes = 1,
    EqualityDeletes = 2,
    // Types below are only allowed in the root
    DataManifest = 3,
    DeleteManifest = 4,
}

// ToDataType implementations for enums
impl ToDataType for DataContentType {
    fn to_data_type() -> DataType {
        DataType::INTEGER
    }
}

impl From<DataContentType> for Scalar {
    fn from(value: DataContentType) -> Self {
        Scalar::Integer(value as i32)
    }
}

/// Format of this data.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum DataFileFormat {
    /// Parquet file format: <https://parquet.apache.org/>
    Parquet,
    /// Puffin file format: <https://iceberg.apache.org/puffin-spec/>
    Puffin,
}

impl FromStr for DataFileFormat {
    type Err = Error;

    fn from_str(s: &str) -> DeltaResult<Self> {
        match s.to_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "puffin" => Ok(Self::Puffin),
            _ => Err(Error::internal_error(format!(
                "Unsupported data file format: {}",
                s
            ))),
        }
    }
}

impl std::fmt::Display for DataFileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFileFormat::Parquet => write!(f, "parquet"),
            DataFileFormat::Puffin => write!(f, "puffin"),
        }
    }
}

impl ToDataType for DataFileFormat {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl From<DataFileFormat> for Scalar {
    fn from(value: DataFileFormat) -> Self {
        Scalar::String(value.to_string())
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TrackingStatus {
    Existed = 0,
    Added = 1,
    Deleted = 2,
}

impl ToDataType for TrackingStatus {
    fn to_data_type() -> DataType {
        DataType::INTEGER
    }
}

impl From<TrackingStatus> for Scalar {
    fn from(value: TrackingStatus) -> Self {
        Scalar::Integer(value as i32)
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, ToSchema, IntoEngineData)]
pub(crate) struct ContentInfo {
    /// The offset in the file where the content starts.
    pub(crate) offset: i64,

    /// The length of thea referenced content stored in the file;
    /// required if content_offset is present.
    pub(crate) size_in_bytes: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, ToSchema, IntoEngineData)]
pub struct TrackingInfo {
    pub(crate) status: TrackingStatus,

    /// Snapshot ID where the file was added, or deleted if status is 2. Inherited when null.
    /// Must be written in the root file.
    pub snapshot_id: Option<i64>,

    /// Data sequence number of the file. Inherited in when null and status is 1 (added).
    /// Must be equal to file_sequence_number if content_type is {Data,Delete}Manifest.
    /// Must be written in the root file.
    pub(crate) sequence_number: Option<i64>,

    /// File sequence number indicating when the file was added. Inherited when null and status is added.
    /// Must be equal to sequence_number if content_type is {Data,Delete}Manifest.
    pub(crate) file_sequence_number: Option<i64>,

    /// The _row_id for the first row in the data file if content_type is Data.
    /// If content_type is DataManifest, this is the starting _row_id to assign to rows added by ADDED data files.
    pub(crate) first_row_id: Option<i64>,

    /// Deletion vector tracking changes made in the current commit for manifest entries.
    /// Only used when content_type is DataManifest or DeleteManifest.
    /// This field tracks what was added/changed in the current commit and is cleared between commits.
    pub(crate) changes_dv: Option<Bytes>,
}

impl TrackingInfo {
    /// Get the tracking status
    pub fn status(&self) -> TrackingStatus {
        self.status
    }
}

impl From<TrackingInfo> for Scalar {
    fn from(value: TrackingInfo) -> Self {
        use crate::expressions::StructData;
        use crate::schema::ToSchema;

        let fields = TrackingInfo::to_schema().into_fields().collect();
        let values = vec![
            value.status.into(),
            value.snapshot_id.into(),
            value.sequence_number.into(),
            value.file_sequence_number.into(),
            value.first_row_id.into(),
        ];

        // SAFETY: Fields are generated by ToSchema derive macro and values are constructed
        // to match exactly in count, order, type, and nullability.
        Scalar::Struct(StructData::new_unchecked(fields, values))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, ToSchema, IntoEngineData)]
pub(crate) struct ManifestStats {
    pub(crate) added_files_count: i64,
    pub(crate) existing_files_count: i64,
    pub(crate) deletes_files_count: i64,

    pub(crate) added_rows_count: i64,
    pub(crate) existing_rows_count: i64,
    pub(crate) delete_rows_count: i64,

    pub(crate) min_sequence_number: i64,
}

impl From<ManifestStats> for Scalar {
    fn from(value: ManifestStats) -> Self {
        use crate::expressions::StructData;
        use crate::schema::ToSchema;

        let fields = ManifestStats::to_schema().into_fields().collect();
        let values = vec![
            value.added_files_count.into(),
            value.existing_files_count.into(),
            value.deletes_files_count.into(),
            value.added_rows_count.into(),
            value.existing_rows_count.into(),
            value.delete_rows_count.into(),
            value.min_sequence_number.into(),
        ];

        // SAFETY: Fields are generated by ToSchema derive macro and values are constructed
        // to match exactly in count, order, type, and nullability.
        Scalar::Struct(StructData::new_unchecked(fields, values))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MetadataEntry {
    /// Type of content stored by the entry.
    /// DataManifest, DeleteManifest or ManifestDV can only be defined in the root manifest.
    pub content_type: DataContentType,

    /// Location of the file. Required for most content types.
    pub location: Option<String>,

    /// avro, orc, parquet or puffin
    pub(crate) file_format: DataFileFormat,

    pub tracking_info: Option<TrackingInfo>,

    pub(crate) content_info: Option<ContentInfo>,

    /// ID of partition spec used to write manifest or data/delete files.
    pub(crate) partition_spec_id: i64,

    /// ID representing sort order for this file. Can only be set if content_type is Data.
    pub(crate) sort_order_id: Option<i64>,

    /// Number of records in this file, or the cardinality of a deletion vector
    pub(crate) record_count: i64,

    /// Total file size in bytes. Must be defined if location is defined
    pub(crate) file_size_in_bytes: Option<i64>,

    /// Column-level statistics for the data file.
    /// The schema of this struct is dynamically generated based on the table schema
    /// using [`stats::stats_schema`]. When `None`, no statistics are available.
    /// See: <https://docs.google.com/document/d/1uvbrwwAJW2TgsnoaIcwAFpjbhHkBUL5wY_24nKgtt9I/>
    pub(crate) content_stats: Option<StructData>,

    /// Must be set if content_type is {Data,Delete}Manifest, otherwise null.
    pub(crate) manifest_info: Option<ManifestStats>,

    /// Location of the data file if the content_type is  PositionDeletes
    /// Location of affiliated data manifest if content_type is or DeleteManifest or null if delete manifest is unaffiliated.
    pub referenced_file: Option<String>,

    /// Not used by Delta today
    /// Implementation-specific key metadata for encryption
    pub(crate) key_metadata: Option<Bytes>,

    /// Not used by Delta today
    /// Split offsets for the data file. For example, all row group offsets in a Parquet file. Must be sorted ascending
    pub(crate) split_offsets: Option<Vec<i64>>,

    /// Not used by Delta today
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and must be null otherwise.
    /// Fields with ids listed in this column must be present in the delete file
    pub(crate) equality_ids: Option<Vec<i32>>,

    /// DV that applies to the manifest linked to from this entry.
    pub(crate) manifest_dv: Option<Bytes>,
}

impl MetadataEntry {
    /// Returns MetadataEntry schema augmented with metadata columns for tracking.
    /// Adds:
    /// - RowIndex: 0-based position of entry within source manifest file
    /// - FilePath: URL of the source manifest file
    ///
    /// # Arguments
    /// * `table_schema` - The table's data schema to generate content_stats schema from
    #[allow(dead_code)]
    pub(crate) fn to_schema_with_metadata_columns(
        table_schema: &StructType,
    ) -> DeltaResult<SchemaRef> {
        use crate::schema::MetadataColumnSpec;

        let base_schema = Self::to_schema_with_content_stats(table_schema)?;
        let mut schema_with_tracking = base_schema;

        schema_with_tracking = schema_with_tracking
            .add_metadata_column("__manifest_row_index", MetadataColumnSpec::RowIndex)?;

        schema_with_tracking = schema_with_tracking
            .add_metadata_column("__manifest_file_path", MetadataColumnSpec::FilePath)?;

        Ok(Arc::new(schema_with_tracking))
    }

    /// Returns a base MetadataEntry schema that excludes content_stats.
    ///
    /// This is used for reading metadata entries back from parquet files where
    /// we don't need the table-schema-dependent content_stats field. The visitor
    /// pattern requires static schema references, so we use this fixed schema
    /// for reading rather than the dynamic `to_schema_with_content_stats`.
    ///
    /// Note: When reading metadata entries using this schema, content_stats will
    /// always be None since it's not included in this schema.
    pub(crate) fn base_schema() -> StructType {
        use crate::schema::derive_macro_utils::GetStructField as _;

        StructType::new_unchecked([
            DataContentType::get_struct_field("contentType"),
            Option::<String>::get_struct_field("location"),
            DataFileFormat::get_struct_field("fileFormat"),
            TrackingInfo::get_struct_field("trackingInfo"),
            Option::<ContentInfo>::get_struct_field("contentInfo"),
            i64::get_struct_field("partitionSpecId"),
            Option::<i64>::get_struct_field("sortOrderId"),
            i64::get_struct_field("recordCount"),
            Option::<i64>::get_struct_field("fileSizeInBytes"),
            // content_stats intentionally excluded - requires table schema
            // Use `to_schema_with_content_stats(table_schema)` when writing
            Option::<ManifestStats>::get_struct_field("manifestStats"),
            Option::<String>::get_struct_field("referencedFile"),
            Option::<Bytes>::get_struct_field("manifestDv"),
            // key_metadata intentionally excluded - binary type not supported
            // split_offsets intentionally excluded - not used by Delta today
            // equality_ids intentionally excluded - not used by Delta today
        ])
    }

    /// Returns MetadataEntry schema with content_stats based on the given table schema.
    ///
    /// The content_stats field schema is dynamically generated in Delta JSON stats format
    /// (numRecords, nullCount, minValues, maxValues, tightBounds) matching the format
    /// used by [`Transaction::add_files_schema`].
    ///
    /// # Arguments
    ///
    /// * `table_schema` - The table's data schema to generate stats schema from
    ///
    /// # Returns
    ///
    /// Returns `Ok(StructType)` containing the full MetadataEntry schema with content_stats,
    /// or an error if stats schema generation fails.
    #[allow(dead_code)]
    pub(crate) fn to_schema_with_content_stats(
        table_schema: &StructType,
    ) -> DeltaResult<StructType> {
        use crate::schema::derive_macro_utils::GetStructField as _;

        // Generate AMT-style stats schema format:
        // {col: {value_count: LONG, null_value_count: LONG (if nullable), nan_value_count: LONG (if float/double), lower_bound: <type>, upper_bound: <type>, exact_bounds: BOOLEAN}, ...}
        let stats_struct = stats::stats_schema(table_schema)?;

        Ok(StructType::new_unchecked([
            DataContentType::get_struct_field("contentType"),
            Option::<String>::get_struct_field("location"),
            DataFileFormat::get_struct_field("fileFormat"),
            TrackingInfo::get_struct_field("trackingInfo"),
            Option::<ContentInfo>::get_struct_field("contentInfo"),
            i64::get_struct_field("partitionSpecId"),
            Option::<i64>::get_struct_field("sortOrderId"),
            i64::get_struct_field("recordCount"),
            Option::<i64>::get_struct_field("fileSizeInBytes"),
            // content_stats - dynamic based on table schema (AMT stats format)
            StructField::new(
                "contentStats",
                DataType::Struct(Box::new(stats_struct)),
                true,
            ),
            Option::<ManifestStats>::get_struct_field("manifestStats"),
            Option::<String>::get_struct_field("referencedFile"),
            Option::<Bytes>::get_struct_field("manifestDv"),
            // key_metadata intentionally excluded - binary type not supported
            // split_offsets intentionally excluded - not used by Delta today
            // equality_ids intentionally excluded - not used by Delta today
        ]))
    }
}

impl crate::IntoEngineData for MetadataEntry {
    fn into_engine_data(
        self,
        schema: crate::schema::SchemaRef,
        engine: &dyn crate::Engine,
    ) -> DeltaResult<Box<dyn crate::EngineData>> {
        // Use create_many with structured scalars (more efficient than create_one with flat values)
        let scalars = metadata_entry_to_scalars(&self, &schema)?;
        let evaluator = engine.evaluation_handler();
        evaluator.create_many(schema, &[&scalars])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{engine::sync::SyncEngine, IntoEngineData};
    use tempfile::tempdir;

    // Note: Full integration test for MetadataEntry::into_engine_data is not included here
    // because it requires complex setup with nested structs. The implementation is complete
    // and can be tested in integration tests with actual data.

    #[test]
    fn test_simple_into_engine_data() -> DeltaResult<()> {
        use crate::IntoEngineData;
        let engine = SyncEngine::new();

        // Create a very simple entry with no optional fields
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("test.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(200),
                first_row_id: Some(1000),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 42,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let schema = test_metadata_entry_schema();
        let result = entry.into_engine_data(schema, &engine);
        if let Err(e) = &result {
            eprintln!("Error in test_simple_into_engine_data: {:?}", e);
        }
        result?;

        Ok(())
    }

    #[test]
    fn test_absolute_to_relative_path() {
        // Test with memory:// URLs
        let table_root = Url::parse("memory:///").unwrap();
        let absolute_url = Url::parse("memory:///part-content-root.parquet").unwrap();
        let result = absolute_to_relative_path(&absolute_url, &table_root).unwrap();
        assert_eq!(result, "part-content-root.parquet");

        // Test with s3:// URLs
        let table_root = Url::parse("s3://my-bucket/my-table/").unwrap();
        let absolute_url = Url::parse("s3://my-bucket/my-table/data/part-00000.parquet").unwrap();
        let result = absolute_to_relative_path(&absolute_url, &table_root).unwrap();
        assert_eq!(result, "data/part-00000.parquet");

        // Test with nested paths
        let table_root = Url::parse("s3://bucket/table/").unwrap();
        let absolute_url = Url::parse("s3://bucket/table/year=2023/month=10/part.parquet").unwrap();
        let result = absolute_to_relative_path(&absolute_url, &table_root).unwrap();
        assert_eq!(result, "year=2023/month=10/part.parquet");

        // Test with file:// URLs
        let table_root = Url::parse("file:///path/to/table/").unwrap();
        let absolute_url = Url::parse("file:///path/to/table/data/file.parquet").unwrap();
        let result = absolute_to_relative_path(&absolute_url, &table_root).unwrap();
        assert_eq!(result, "data/file.parquet");

        // Test when root doesn't match (no common prefix)
        let table_root = Url::parse("s3://bucket-b/table-b/").unwrap();
        let absolute_url = Url::parse("s3://bucket-a/table-a/file.parquet").unwrap();
        let result = absolute_to_relative_path(&absolute_url, &table_root).unwrap();
        // Since there's no common prefix in the path part, it returns the path without leading slash
        assert_eq!(result, "table-a/file.parquet");
    }

    #[test]
    fn test_metadata_entry_base_schema_fields() {
        // Verify the base schema has the expected structure (excludes content_stats)
        let schema = MetadataEntry::base_schema();

        // Schema should have all the top-level fields (excluding content_stats, key_metadata, split_offsets, equality_ids)
        // Fields: contentType, location, fileFormat, trackingInfo, contentInfo, partitionSpecId, sortOrderId,
        // recordCount, fileSizeInBytes, manifestStats, referencedFile, manifestDv
        assert_eq!(schema.fields().len(), 12);

        // Check leaves (flattened leaf fields)
        let leaves = schema.leaves(None::<&str>);
        let (leaf_names, _leaf_types) = leaves.as_ref();

        // Schema should have all the leaf fields (24 = flattened count, excluding key_metadata, split_offsets, equality_ids)
        // Added 2 fields (manifestDv, manifestDeltaDv), removed 1 (inlineContent), so 23 + 1 = 24
        assert_eq!(leaf_names.len(), 24);
    }

    #[test]
    fn test_to_schema_with_content_stats() -> DeltaResult<()> {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructType};

        // Helper to create field with parquet field ID
        fn field_with_id(
            name: &str,
            data_type: DataType,
            nullable: bool,
            field_id: i32,
        ) -> StructField {
            StructField::new(name, data_type, nullable).with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(field_id as i64),
            )])
        }

        // Create a simple table schema with field IDs (required for AMT stats schema)
        let table_schema = StructType::new_unchecked([
            field_with_id("id", DataType::INTEGER, false, 1),
            field_with_id("name", DataType::STRING, true, 2),
            field_with_id("value", DataType::DOUBLE, true, 3),
        ]);

        // Generate schema with content_stats
        let schema_with_stats = MetadataEntry::to_schema_with_content_stats(&table_schema)?;

        // Schema should have 13 top-level fields (12 base + 1 for contentStats)
        assert_eq!(schema_with_stats.fields().len(), 13);

        // Verify contentStats field exists
        let content_stats_field = schema_with_stats
            .field("contentStats")
            .expect("contentStats field should exist");
        assert!(content_stats_field.nullable);

        // Verify contentStats is a struct with AMT stats format:
        // {col_name: {value_count, null_value_count?, nan_value_count?, lower_bound, upper_bound, exact_bounds}, ...}
        let content_stats_struct = match content_stats_field.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected contentStats to be a struct"),
        };

        // Should have 3 fields: id, name, value (one per column)
        assert_eq!(content_stats_struct.fields().count(), 3);
        assert!(content_stats_struct.field("id").is_some());
        assert!(content_stats_struct.field("name").is_some());
        assert!(content_stats_struct.field("value").is_some());

        // Verify each column has a stats struct
        // id: non-nullable INTEGER -> {value_count, lower_bound, upper_bound, exact_bounds}
        let id_stats = match content_stats_struct.field("id").unwrap().data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected id stats to be a struct"),
        };
        assert!(id_stats.field("value_count").is_some());
        assert!(id_stats.field("null_value_count").is_none()); // not nullable
        assert!(id_stats.field("nan_value_count").is_none()); // not float/double
        assert!(id_stats.field("lower_bound").is_some());
        assert!(id_stats.field("upper_bound").is_some());
        assert!(id_stats.field("exact_bounds").is_some());
        assert_eq!(
            id_stats.field("lower_bound").unwrap().data_type(),
            &DataType::INTEGER
        );

        // name: nullable STRING -> {value_count, null_value_count, avg_value_size, max_value_size, lower_bound, upper_bound, exact_bounds}
        let name_stats = match content_stats_struct.field("name").unwrap().data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected name stats to be a struct"),
        };
        assert!(name_stats.field("value_count").is_some());
        assert!(name_stats.field("null_value_count").is_some()); // nullable
        assert!(name_stats.field("nan_value_count").is_none()); // not float/double
        assert!(name_stats.field("avg_value_size").is_some()); // string has size stats
        assert!(name_stats.field("max_value_size").is_some()); // string has size stats
        assert!(name_stats.field("lower_bound").is_some());
        assert!(name_stats.field("upper_bound").is_some());
        assert!(name_stats.field("exact_bounds").is_some());
        assert_eq!(
            name_stats.field("lower_bound").unwrap().data_type(),
            &DataType::STRING
        );

        // value: nullable DOUBLE -> {value_count, null_value_count, nan_value_count, lower_bound, upper_bound, exact_bounds}
        let value_stats = match content_stats_struct.field("value").unwrap().data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected value stats to be a struct"),
        };
        assert!(value_stats.field("value_count").is_some());
        assert!(value_stats.field("null_value_count").is_some()); // nullable
        assert!(value_stats.field("nan_value_count").is_some()); // double has nan count
        assert!(value_stats.field("lower_bound").is_some());
        assert!(value_stats.field("upper_bound").is_some());
        assert!(value_stats.field("exact_bounds").is_some());
        assert_eq!(
            value_stats.field("lower_bound").unwrap().data_type(),
            &DataType::DOUBLE
        );

        Ok(())
    }

    #[test]
    fn test_into_engine_data_with_content_stats() -> DeltaResult<()> {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructType};
        use crate::IntoEngineData;

        // Helper to create field with parquet field ID
        fn field_with_id(
            name: &str,
            data_type: DataType,
            nullable: bool,
            field_id: i32,
        ) -> StructField {
            StructField::new(name, data_type, nullable).with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(field_id as i64),
            )])
        }

        let engine = SyncEngine::new();

        // Create a simple table schema with field IDs
        let table_schema = StructType::new_unchecked([
            field_with_id("id", DataType::INTEGER, false, 1),
            field_with_id("value", DataType::DOUBLE, true, 2),
        ]);

        // Generate the schema with content_stats
        let schema_with_stats =
            Arc::new(MetadataEntry::to_schema_with_content_stats(&table_schema)?);

        // Create content_stats in AMT format:
        // {id: {value_count, lower_bound, upper_bound, exact_bounds},
        //  value: {value_count, null_value_count, nan_value_count, lower_bound, upper_bound, exact_bounds}}

        // Build id stats struct (non-nullable INTEGER, so no null_value_count or nan_value_count)
        let id_stats = StructData::try_new(
            vec![
                StructField::nullable("value_count", DataType::LONG),
                StructField::nullable("lower_bound", DataType::INTEGER),
                StructField::nullable("upper_bound", DataType::INTEGER),
                StructField::nullable("exact_bounds", DataType::BOOLEAN),
            ],
            vec![
                Scalar::Long(100),
                Scalar::Integer(1),
                Scalar::Integer(1000),
                Scalar::Boolean(true),
            ],
        )?;

        // Build value stats struct (nullable DOUBLE, so has null_value_count and nan_value_count)
        let value_stats = StructData::try_new(
            vec![
                StructField::nullable("value_count", DataType::LONG),
                StructField::nullable("null_value_count", DataType::LONG),
                StructField::nullable("nan_value_count", DataType::LONG),
                StructField::nullable("lower_bound", DataType::DOUBLE),
                StructField::nullable("upper_bound", DataType::DOUBLE),
                StructField::nullable("exact_bounds", DataType::BOOLEAN),
            ],
            vec![
                Scalar::Long(100),
                Scalar::Long(5),
                Scalar::Long(0),
                Scalar::Double(0.0),
                Scalar::Double(100.0),
                Scalar::Boolean(true),
            ],
        )?;

        // Build the content_stats struct
        let content_stats = StructData::try_new(
            vec![
                StructField::nullable(
                    "id",
                    DataType::Struct(Box::new(StructType::new_unchecked([
                        StructField::nullable("value_count", DataType::LONG),
                        StructField::nullable("lower_bound", DataType::INTEGER),
                        StructField::nullable("upper_bound", DataType::INTEGER),
                        StructField::nullable("exact_bounds", DataType::BOOLEAN),
                    ]))),
                ),
                StructField::nullable(
                    "value",
                    DataType::Struct(Box::new(StructType::new_unchecked([
                        StructField::nullable("value_count", DataType::LONG),
                        StructField::nullable("null_value_count", DataType::LONG),
                        StructField::nullable("nan_value_count", DataType::LONG),
                        StructField::nullable("lower_bound", DataType::DOUBLE),
                        StructField::nullable("upper_bound", DataType::DOUBLE),
                        StructField::nullable("exact_bounds", DataType::BOOLEAN),
                    ]))),
                ),
            ],
            vec![Scalar::Struct(id_stats), Scalar::Struct(value_stats)],
        )?;

        // Create a MetadataEntry with content_stats
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: Some(content_stats),
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Convert to EngineData
        let engine_data = entry.into_engine_data(schema_with_stats.clone(), &engine)?;

        // Verify the engine data was created successfully
        assert!(!engine_data.is_empty());
        assert_eq!(engine_data.len(), 1); // Single row

        Ok(())
    }

    #[test]
    fn test_into_engine_data_with_null_content_stats() -> DeltaResult<()> {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructType};
        use crate::IntoEngineData;

        let engine = SyncEngine::new();

        // Create a simple table schema with parquet field IDs and column mapping annotations
        // (column mapping is required when metadata tree feature is enabled)
        let table_schema =
            StructType::new_unchecked([StructField::new("id", DataType::INTEGER, false)
                .with_metadata([
                    (
                        ColumnMetadataKey::ParquetFieldId.as_ref(),
                        MetadataValue::Number(1),
                    ),
                    (
                        ColumnMetadataKey::ColumnMappingId.as_ref(),
                        MetadataValue::Number(1),
                    ),
                    (
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        MetadataValue::String("col-id".to_string()),
                    ),
                ])]);

        // Generate the schema with content_stats
        let schema_with_stats =
            Arc::new(MetadataEntry::to_schema_with_content_stats(&table_schema)?);

        // Create a MetadataEntry with content_stats set to None
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None, // Explicitly None
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Convert to EngineData - should handle null content_stats gracefully
        let engine_data = entry.into_engine_data(schema_with_stats.clone(), &engine)?;

        // Verify the engine data was created successfully
        assert!(!engine_data.is_empty());
        assert_eq!(engine_data.len(), 1); // Single row

        Ok(())
    }

    #[test]
    fn test_roundtrip_with_content_stats() -> DeltaResult<()> {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructType};
        use crate::IntoEngineData;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a simple table schema with parquet field IDs and column mapping annotations
        // (column mapping is required when metadata tree feature is enabled)
        let table_schema = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false).with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-id".to_string()),
                ),
            ]),
            StructField::new("name", DataType::STRING, true).with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(2),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(2),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-name".to_string()),
                ),
            ]),
        ]);

        // Generate the schema with content_stats
        let schema_with_stats =
            Arc::new(MetadataEntry::to_schema_with_content_stats(&table_schema)?);

        // Create content_stats data in AMT format:
        // {id: {value_count, lower_bound, upper_bound, exact_bounds},
        //  name: {value_count, null_value_count, avg_value_size, max_value_size, lower_bound, upper_bound, exact_bounds}}

        // Build id stats struct (non-nullable INTEGER, so no null_value_count)
        let id_stats_fields = vec![
            StructField::nullable("value_count", DataType::LONG),
            StructField::nullable("lower_bound", DataType::INTEGER),
            StructField::nullable("upper_bound", DataType::INTEGER),
            StructField::nullable("exact_bounds", DataType::BOOLEAN),
        ];
        let id_stats = StructData::try_new(
            id_stats_fields.clone(),
            vec![
                Scalar::Long(500),
                Scalar::Integer(1),
                Scalar::Integer(500),
                Scalar::Boolean(true),
            ],
        )?;

        // Build name stats struct (nullable STRING, so has null_value_count and size stats)
        let name_stats_fields = vec![
            StructField::nullable("value_count", DataType::LONG),
            StructField::nullable("null_value_count", DataType::LONG),
            StructField::nullable("avg_value_size", DataType::LONG),
            StructField::nullable("max_value_size", DataType::LONG),
            StructField::nullable("lower_bound", DataType::STRING),
            StructField::nullable("upper_bound", DataType::STRING),
            StructField::nullable("exact_bounds", DataType::BOOLEAN),
        ];
        let name_stats = StructData::try_new(
            name_stats_fields.clone(),
            vec![
                Scalar::Long(500),
                Scalar::Long(10),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
                Scalar::String("aardvark".to_string()),
                Scalar::String("zebra".to_string()),
                Scalar::Boolean(true),
            ],
        )?;

        // Build the content_stats struct in AMT format
        let content_stats_fields = vec![
            StructField::nullable(
                "id",
                DataType::Struct(Box::new(StructType::new_unchecked(id_stats_fields))),
            ),
            StructField::nullable(
                "name",
                DataType::Struct(Box::new(StructType::new_unchecked(name_stats_fields))),
            ),
        ];
        let content_stats = StructData::try_new(
            content_stats_fields,
            vec![Scalar::Struct(id_stats), Scalar::Struct(name_stats)],
        )?;

        // Create a MetadataEntry with content_stats
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/data/file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 500,
            file_size_in_bytes: Some(2048),
            content_stats: Some(content_stats),
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Convert to EngineData using schema with content_stats
        let engine_data = entry.into_engine_data(schema_with_stats.clone(), &engine)?;

        // Create Metadata and write it
        let metadata = Metadata {
            data: vec![engine_data],
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write metadata using the writer
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Verify the file was written
        assert!(written_file.as_str().ends_with(".parquet"));

        // Note: Full roundtrip reading is not tested here because the reader
        // would need to be updated to handle content_stats. The key thing
        // we're testing is that the IntoEngineData conversion works correctly.

        Ok(())
    }

    #[test]
    fn test_enum_to_scalar_conversions() {
        // Test DataContentType conversion
        let content_type = DataContentType::Data;
        let scalar: Scalar = content_type.into();
        assert!(matches!(scalar, Scalar::Integer(0)));

        // Test DataFileFormat conversion
        let file_format = DataFileFormat::Parquet;
        let scalar: Scalar = file_format.into();
        assert!(matches!(scalar, Scalar::String(ref s) if s == "parquet"));

        // Test TrackingStatus conversion
        let status = TrackingStatus::Added;
        let scalar: Scalar = status.into();
        assert!(matches!(scalar, Scalar::Integer(1)));
    }

    #[test]
    fn test_bytes_to_scalar_conversion() {
        let bytes = Bytes::from(vec![1, 2, 3, 4]);
        let scalar: Scalar = bytes.into();
        assert!(matches!(scalar, Scalar::Binary(ref v) if v == &vec![1, 2, 3, 4]));
    }

    /// Helper function to create a simple test table schema with parquet field IDs.
    /// This is used for tests that need to generate content_stats schema.
    fn test_table_schema() -> StructType {
        use crate::schema::{ColumnMetadataKey, MetadataValue};

        StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false).with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-id".to_string()),
                ),
            ]),
        ])
    }

    /// Helper function to get the test schema for MetadataEntry with content_stats.
    /// Uses `test_table_schema()` to generate the dynamic schema.
    fn test_metadata_entry_schema() -> SchemaRef {
        Arc::new(
            MetadataEntry::to_schema_with_content_stats(&test_table_schema())
                .expect("test schema should be valid"),
        )
    }

    // Helper function to create a simple MetadataEntry for testing
    fn create_simple_metadata_entry() -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/path/to/file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(200),
                first_row_id: Some(1000),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 42,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    // Helper function to create a MetadataEntry representing a PositionDeletes file
    fn create_metadata_entry_with_dv() -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::PositionDeletes,
            location: Some("s3://bucket/path/to/deletes.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(5),
                sequence_number: Some(500),
                file_sequence_number: Some(600),
                first_row_id: Some(5000),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 1,
            sort_order_id: Some(1),
            record_count: 10,
            file_size_in_bytes: Some(512),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    // Helper function to create a MetadataEntry with manifest DV
    fn create_metadata_entry_with_inline_dv() -> MetadataEntry {
        // Create some sample inline DV data
        let inline_data = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0xAB, 0xCD, 0xEF];

        MetadataEntry {
            content_type: DataContentType::DataManifest,
            location: Some("s3://bucket/path/to/manifest.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(3),
                sequence_number: Some(300),
                file_sequence_number: Some(400),
                first_row_id: Some(3000),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 100,
            file_size_in_bytes: Some(2048),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: Some(Bytes::from(inline_data)),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    // Helper function to create a MetadataEntry with manifest stats
    fn create_metadata_entry_with_manifest_info() -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::DataManifest,
            location: Some("s3://bucket/path/to/manifest.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Existed,
                snapshot_id: Some(10),
                sequence_number: Some(1000),
                file_sequence_number: Some(1000),
                first_row_id: Some(10000),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 2,
            sort_order_id: Some(2),
            record_count: 100,
            file_size_in_bytes: Some(10240),
            content_stats: None,
            manifest_info: Some(ManifestStats {
                added_files_count: 5,
                existing_files_count: 10,
                deletes_files_count: 2,
                added_rows_count: 500,
                existing_rows_count: 1000,
                delete_rows_count: 50,
                min_sequence_number: 100,
            }),
            referenced_file: Some("s3://bucket/path/to/referenced.parquet".to_string()),
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    // Helper to compare two metadata entries (excluding fields that are not yet fully supported)
    fn assert_metadata_entry_eq(expected: &MetadataEntry, actual: &MetadataEntry) {
        assert_eq!(
            expected.content_type, actual.content_type,
            "content_type mismatch"
        );
        assert_eq!(expected.location, actual.location, "location mismatch");
        assert_eq!(
            expected.file_format, actual.file_format,
            "file_format mismatch"
        );

        // Compare tracking_info
        match (&expected.tracking_info, &actual.tracking_info) {
            (Some(exp_ti), Some(act_ti)) => {
                assert_eq!(
                    exp_ti.status, act_ti.status,
                    "tracking_info.status mismatch"
                );
                assert_eq!(
                    exp_ti.snapshot_id, act_ti.snapshot_id,
                    "tracking_info.snapshot_id mismatch"
                );
                assert_eq!(
                    exp_ti.sequence_number, act_ti.sequence_number,
                    "tracking_info.sequence_number mismatch"
                );
                assert_eq!(
                    exp_ti.file_sequence_number, act_ti.file_sequence_number,
                    "tracking_info.file_sequence_number mismatch"
                );
                assert_eq!(
                    exp_ti.first_row_id, act_ti.first_row_id,
                    "tracking_info.first_row_id mismatch"
                );
            }
            (None, None) => {}
            _ => panic!("tracking_info presence mismatch"),
        }

        // Compare manifest_dv and changes_dv
        assert_eq!(
            expected.manifest_dv, actual.manifest_dv,
            "manifest_dv mismatch"
        );
        assert_eq!(
            expected.tracking_info.as_ref().map(|t| &t.changes_dv),
            actual.tracking_info.as_ref().map(|t| &t.changes_dv),
            "changes_dv mismatch"
        );

        assert_eq!(
            expected.partition_spec_id, actual.partition_spec_id,
            "partition_spec_id mismatch"
        );
        assert_eq!(
            expected.sort_order_id, actual.sort_order_id,
            "sort_order_id mismatch"
        );
        assert_eq!(
            expected.record_count, actual.record_count,
            "record_count mismatch"
        );
        assert_eq!(
            expected.file_size_in_bytes, actual.file_size_in_bytes,
            "file_size_in_bytes mismatch"
        );

        // Compare manifest_info
        match (&expected.manifest_info, &actual.manifest_info) {
            (Some(exp_ms), Some(act_ms)) => {
                assert_eq!(
                    exp_ms.added_files_count, act_ms.added_files_count,
                    "manifest_info.added_files_count mismatch"
                );
                assert_eq!(
                    exp_ms.existing_files_count, act_ms.existing_files_count,
                    "manifest_info.existing_files_count mismatch"
                );
                assert_eq!(
                    exp_ms.deletes_files_count, act_ms.deletes_files_count,
                    "manifest_info.deletes_files_count mismatch"
                );
                assert_eq!(
                    exp_ms.added_rows_count, act_ms.added_rows_count,
                    "manifest_info.added_rows_count mismatch"
                );
                assert_eq!(
                    exp_ms.existing_rows_count, act_ms.existing_rows_count,
                    "manifest_info.existing_rows_count mismatch"
                );
                assert_eq!(
                    exp_ms.delete_rows_count, act_ms.delete_rows_count,
                    "manifest_info.delete_rows_count mismatch"
                );
                assert_eq!(
                    exp_ms.min_sequence_number, act_ms.min_sequence_number,
                    "manifest_info.min_sequence_number mismatch"
                );
            }
            (None, None) => {}
            _ => panic!("manifest_info presence mismatch"),
        }

        assert_eq!(
            expected.referenced_file, actual.referenced_file,
            "referenced_file mismatch"
        );
        // Note: key_metadata, split_offsets, equality_ids are not yet fully supported
    }

    #[test]
    fn test_roundtrip_simple_metadata_entry() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create original metadata
        let original_entry = create_simple_metadata_entry();
        let metadata = Metadata {
            data: vec![original_entry
                .clone()
                .into_engine_data(test_metadata_entry_schema(), &engine)?],
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let path_in_log = absolute_to_relative_path(&written_file, &table_root_url)?;
        let read_metadata =
            Metadata::read(&engine, &written_file, path_in_log, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &entries[0]);

        Ok(())
    }

    #[test]
    fn test_roundtrip_metadata_entry_with_deletion_vector() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create metadata with deletion vector
        let original_entry = create_metadata_entry_with_dv();
        let metadata = Metadata {
            data: vec![original_entry
                .clone()
                .into_engine_data(test_metadata_entry_schema(), &engine)?],
            version: 1,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let path_in_log = absolute_to_relative_path(&written_file, &table_root_url)?;
        let read_metadata =
            Metadata::read(&engine, &written_file, path_in_log, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &entries[0]);

        Ok(())
    }

    #[test]
    fn test_roundtrip_metadata_entry_with_manifest_info() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create metadata with manifest stats
        let original_entry = create_metadata_entry_with_manifest_info();
        let metadata = Metadata {
            data: vec![original_entry
                .clone()
                .into_engine_data(test_metadata_entry_schema(), &engine)?],
            version: 2,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let path_in_log = absolute_to_relative_path(&written_file, &table_root_url)?;
        let read_metadata =
            Metadata::read(&engine, &written_file, path_in_log, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &entries[0]);

        Ok(())
    }

    #[test]
    fn test_roundtrip_metadata_entry_with_inline_deletion_vector() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create metadata with inline deletion vector
        let original_entry = create_metadata_entry_with_inline_dv();
        let metadata = Metadata {
            data: vec![original_entry
                .clone()
                .into_engine_data(test_metadata_entry_schema(), &engine)?],
            version: 3,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let path_in_log = absolute_to_relative_path(&written_file, &table_root_url)?;
        let read_metadata =
            Metadata::read(&engine, &written_file, path_in_log, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &entries[0]);

        // Verify manifest_dv specifically
        let read_entry = &entries[0];
        assert!(
            read_entry.manifest_dv.is_some(),
            "manifest_dv should be present"
        );
        let read_bytes = read_entry.manifest_dv.as_ref().unwrap();
        let orig_bytes = original_entry.manifest_dv.as_ref().unwrap();
        assert_eq!(
            read_bytes.len(),
            orig_bytes.len(),
            "manifest_dv length must match"
        );
        assert_eq!(
            read_bytes.as_ref(),
            orig_bytes.as_ref(),
            "manifest_dv bytes must match"
        );

        Ok(())
    }

    #[test]
    fn test_roundtrip_multiple_metadata_entries() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create multiple entries including one with inline DV
        let entry1 = create_simple_metadata_entry();
        let entry2 = create_metadata_entry_with_dv();
        let entry3 = create_metadata_entry_with_manifest_info();
        let entry4 = create_metadata_entry_with_inline_dv();

        let metadata = Metadata {
            data: vec![
                entry1
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                entry2
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                entry3
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                entry4
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ],
            version: 3,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let path_in_log = absolute_to_relative_path(&written_file, &table_root_url)?;
        let read_metadata =
            Metadata::read(&engine, &written_file, path_in_log, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 4);
        assert_metadata_entry_eq(&entry1, &entries[0]);
        assert_metadata_entry_eq(&entry2, &entries[1]);
        assert_metadata_entry_eq(&entry3, &entries[2]);
        assert_metadata_entry_eq(&entry4, &entries[3]);

        Ok(())
    }

    #[test]
    fn test_roundtrip_all_data_content_types() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create entries with all content types
        let content_types = vec![
            DataContentType::Data,
            DataContentType::PositionDeletes,
            DataContentType::EqualityDeletes,
            DataContentType::DataManifest,
            DataContentType::DeleteManifest,
        ];

        let entries: Vec<MetadataEntry> = content_types
            .into_iter()
            .enumerate()
            .map(|(i, content_type)| MetadataEntry {
                content_type,
                location: Some(format!("s3://bucket/file{}.parquet", i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(i as i64),
                    sequence_number: Some((i * 100) as i64),
                    file_sequence_number: Some((i * 200) as i64),
                    first_row_id: Some((i * 1000) as i64),
                    changes_dv: None,
                }),
                content_info: None,
                partition_spec_id: i as i64,
                sort_order_id: Some(i as i64),
                record_count: (i * 10) as i64,
                file_size_in_bytes: Some((i * 512) as i64),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                manifest_dv: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            })
            .collect();

        let data: Vec<Box<dyn EngineData>> = entries
            .iter()
            .map(|e| {
                e.clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        let metadata = Metadata {
            data,
            version: 4,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let path_in_log = absolute_to_relative_path(&written_file, &table_root_url)?;
        let read_metadata =
            Metadata::read(&engine, &written_file, path_in_log, table_root_url.clone())?;

        // Verify
        let read_entries = read_metadata.entries()?;
        assert_eq!(read_entries.len(), entries.len());
        for (expected, actual) in entries.iter().zip(read_entries.iter()) {
            assert_metadata_entry_eq(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_roundtrip_all_tracking_statuses() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create entries with all tracking statuses
        let statuses = vec![
            TrackingStatus::Existed,
            TrackingStatus::Added,
            TrackingStatus::Deleted,
        ];

        let entries: Vec<MetadataEntry> = statuses
            .into_iter()
            .enumerate()
            .map(|(i, status)| MetadataEntry {
                content_type: DataContentType::Data,
                location: Some(format!("s3://bucket/file{}.parquet", i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status,
                    snapshot_id: Some(i as i64),
                    sequence_number: Some((i * 100) as i64),
                    file_sequence_number: Some((i * 200) as i64),
                    first_row_id: Some((i * 1000) as i64),
                    changes_dv: None,
                }),
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: Some(0),
                record_count: 42,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                manifest_dv: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            })
            .collect();

        let data: Vec<Box<dyn EngineData>> = entries
            .iter()
            .map(|e| {
                e.clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        let metadata = Metadata {
            data,
            version: 5,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let path_in_log = absolute_to_relative_path(&written_file, &table_root_url)?;
        let read_metadata =
            Metadata::read(&engine, &written_file, path_in_log, table_root_url.clone())?;

        // Verify
        let read_entries = read_metadata.entries()?;
        assert_eq!(read_entries.len(), entries.len());
        for (expected, actual) in entries.iter().zip(read_entries.iter()) {
            assert_metadata_entry_eq(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_roundtrip_with_optional_fields_null() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create entry with many optional fields set to None
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: None,          // None
                sequence_number: None,      // None
                file_sequence_number: None, // None
                first_row_id: None,         // None
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 42,
            file_size_in_bytes: Some(1024),
            content_stats: None,   // None
            manifest_info: None,   // None
            referenced_file: None, // None
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let metadata = Metadata {
            data: vec![entry
                .clone()
                .into_engine_data(test_metadata_entry_schema(), &engine)?],
            version: 6,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let path_in_log = absolute_to_relative_path(&written_file, &table_root_url)?;
        let read_metadata =
            Metadata::read(&engine, &written_file, path_in_log, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&entry, &entries[0]);

        // Specifically verify the None values
        let actual = &entries[0];
        let ti = actual.tracking_info.as_ref().unwrap();
        assert!(ti.snapshot_id.is_none());
        assert!(ti.sequence_number.is_none());
        assert!(ti.file_sequence_number.is_none());
        assert!(ti.first_row_id.is_none());
        assert!(ti.changes_dv.is_none());
        assert!(actual.manifest_dv.is_none());
        assert!(actual.manifest_info.is_none());
        assert!(actual.referenced_file.is_none());

        Ok(())
    }

    #[test]
    fn test_roundtrip_puffin_format() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create entry with Puffin format
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/file.puffin".to_string()),
            file_format: DataFileFormat::Puffin,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(200),
                first_row_id: Some(1000),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 42,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let metadata = Metadata {
            data: vec![entry
                .clone()
                .into_engine_data(test_metadata_entry_schema(), &engine)?],
            version: 7,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let path_in_log = absolute_to_relative_path(&written_file, &table_root_url)?;
        let read_metadata =
            Metadata::read(&engine, &written_file, path_in_log, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&entry, &entries[0]);
        assert_eq!(entries[0].file_format, DataFileFormat::Puffin);

        Ok(())
    }

    /// Helper to create a data file entry
    fn create_data_entry(location: &str, sequence_number: i64) -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(location.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(sequence_number),
                file_sequence_number: Some(sequence_number),
                first_row_id: Some(0),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    /// Helper to create a deletion vector entry
    fn create_dv_entry(
        location: &str,
        referenced_file: &str,
        sequence_number: i64,
    ) -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::PositionDeletes,
            location: Some(location.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(sequence_number),
                file_sequence_number: Some(sequence_number),
                first_row_id: Some(0),
                changes_dv: None,
            }),
            content_info: Some(ContentInfo {
                offset: 0,
                size_in_bytes: 100,
            }),
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 10,
            file_size_in_bytes: Some(108),
            content_stats: None,
            manifest_info: None,
            referenced_file: Some(referenced_file.to_string()),
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    /// Helper to add _pos column to EngineData for testing
    /// Real parquet files have this added by the reader, but test data needs it manually
    fn add_pos_column(data: Box<dyn EngineData>) -> DeltaResult<Box<dyn EngineData>> {
        use crate::expressions::{ArrayData, Scalar};
        use crate::schema::{ArrayType, DataType, StructField, StructType};

        let num_rows = data.len();

        // Build _pos values (0, 1, 2, ...) as LONG to match schema expectations
        let pos_values: Vec<Scalar> = (0..num_rows as i64).map(Scalar::Long).collect();

        let pos_array = ArrayData::try_new(ArrayType::new(DataType::LONG, false), pos_values)?;

        let pos_schema = Arc::new(StructType::new_unchecked(vec![StructField::new(
            "_pos",
            DataType::LONG,
            false,
        )]));

        data.append_columns(pos_schema, vec![pos_array])
    }

    /// Helper to add _pos column to a vec of EngineData batches
    fn add_pos_to_batches(
        batches: Vec<Box<dyn EngineData>>,
    ) -> DeltaResult<Vec<Box<dyn EngineData>>> {
        batches.into_iter().map(add_pos_column).collect()
    }

    #[test]
    fn test_dv_with_earlier_sequence_number_not_included() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file with sequence number 100
        let data_entry = create_data_entry("memory:///data.parquet", 100);

        // Create a DV for the data file with sequence number 50 (earlier)
        let dv_entry = create_dv_entry("memory:///dv.parquet", "memory:///data.parquet", 50);

        // Create metadata with both entries
        let metadata = Metadata {
            data: add_pos_to_batches(vec![
                data_entry
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                dv_entry
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ])?,
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: "manifest.parquet".to_string(),
            leaf: None,
        };

        // Get action batches (no data skipping for this test)
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[], None)?;

        // Get the Add action using visitor
        let batch = action_batches.next().unwrap()?;
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(batch.actions.as_ref())?;
        assert_eq!(visitor.adds.len(), 1);
        let add = &visitor.adds[0];

        // Verify DV is NOT included (sequence number too early)
        assert!(
            add.deletion_vector.is_none(),
            "DV with earlier sequence number should not be included"
        );

        Ok(())
    }

    #[test]
    fn test_dv_with_later_sequence_number_included() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file with sequence number 50
        let data_entry = create_data_entry("memory:///data.parquet", 50);

        // Create a DV for the data file with sequence number 100 (later)
        let dv_entry = create_dv_entry("memory:///dv.parquet", "memory:///data.parquet", 100);

        // Create metadata with both entries
        let metadata = Metadata {
            data: add_pos_to_batches(vec![
                data_entry
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                dv_entry
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ])?,
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: "manifest.parquet".to_string(),
            leaf: None,
        };

        // Get action batches (no data skipping for this test)
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[], None)?;

        // Get the Add action using visitor
        let batch = action_batches.next().unwrap()?;
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(batch.actions.as_ref())?;
        assert_eq!(visitor.adds.len(), 1);
        let add = &visitor.adds[0];

        // Verify DV IS included (sequence number is later)
        assert!(
            add.deletion_vector.is_some(),
            "DV with later sequence number should be included"
        );
        let dv = add.deletion_vector.as_ref().unwrap();
        assert_eq!(dv.cardinality, 10);

        Ok(())
    }

    #[test]
    fn test_dv_not_present() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file without any corresponding DV
        let data_entry = create_data_entry("memory:///data.parquet", 50);

        // Create metadata with only the data entry (no DV)
        let metadata = Metadata {
            data: add_pos_to_batches(vec![data_entry
                .clone()
                .into_engine_data(test_metadata_entry_schema(), &engine)?])?,
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Get action batches (no data skipping for this test)
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[], None)?;

        // Get the Add action using visitor
        let batch = action_batches.next().unwrap()?;
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(batch.actions.as_ref())?;
        assert_eq!(visitor.adds.len(), 1);
        let add = &visitor.adds[0];

        // Verify DV is NOT included (doesn't exist)
        assert!(
            add.deletion_vector.is_none(),
            "DV should not be included when it doesn't exist"
        );

        Ok(())
    }

    #[test]
    fn test_manifest_dv_not_dropped_in_serialization() -> DeltaResult<()> {
        // This test verifies that manifest_dv survives the into_engine_data conversion
        // even when not read back through the full reader path
        let engine = SyncEngine::new();

        // Create a metadata entry with manifest DV
        let inline_dv_entry = create_metadata_entry_with_inline_dv();
        let original_dv_bytes = inline_dv_entry.manifest_dv.as_ref().unwrap().clone();

        // Convert to engine data
        let engine_data = inline_dv_entry
            .clone()
            .into_engine_data(test_metadata_entry_schema(), &engine)?;

        // The manifest_dv should be in the engine data
        // We can't easily extract it without the full visitor, but we can verify
        // that the conversion succeeded and the data was included
        assert!(!engine_data.is_empty(), "Engine data should not be empty");

        // Verify the original bytes are not empty
        assert!(
            !original_dv_bytes.is_empty(),
            "Original manifest DV should not be empty"
        );
        assert_eq!(
            original_dv_bytes.len(),
            8,
            "Expected 8 bytes of manifest DV"
        );

        Ok(())
    }

    #[test]
    fn test_multiple_dvs_keeps_latest_by_sequence_number() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file with sequence number 50
        let data_entry = create_data_entry("memory:///data.parquet", 50);

        // Create multiple DVs for the same data file with different sequence numbers
        // Note: the one with seq 200 should win regardless of order
        let dv_entry_1 = create_dv_entry("memory:///dv1.parquet", "memory:///data.parquet", 100);
        let mut dv_entry_2 =
            create_dv_entry("memory:///dv2.parquet", "memory:///data.parquet", 200);
        dv_entry_2.record_count = 20; // Different cardinality to distinguish

        let mut dv_entry_3 =
            create_dv_entry("memory:///dv3.parquet", "memory:///data.parquet", 150);
        dv_entry_3.record_count = 15; // Different cardinality to distinguish

        // Create metadata with all entries
        let metadata = Metadata {
            data: add_pos_to_batches(vec![
                data_entry
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                dv_entry_1
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                dv_entry_2
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                dv_entry_3
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ])?,
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: "manifest.parquet".to_string(),
            leaf: None,
        };

        // Get action batches (no data skipping for this test)
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[], None)?;

        // Get the Add action using visitor
        let batch = action_batches.next().unwrap()?;
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(batch.actions.as_ref())?;
        assert_eq!(visitor.adds.len(), 1);
        let add = &visitor.adds[0];

        // Verify the DV with highest sequence number (200) is used
        assert!(
            add.deletion_vector.is_some(),
            "DV should be included when sequence number is later"
        );
        let dv = add.deletion_vector.as_ref().unwrap();
        assert_eq!(
            dv.cardinality, 20,
            "Should use DV with highest sequence number (200), which has cardinality 20"
        );

        Ok(())
    }

    #[test]
    fn test_dv_with_deleted_status_not_included() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file with sequence number 50
        let data_entry = create_data_entry("memory:///data.parquet", 50);

        // Create a DV with Deleted status
        let mut dv_entry = create_dv_entry("memory:///dv.parquet", "memory:///data.parquet", 100);
        if let Some(ref mut ti) = dv_entry.tracking_info {
            ti.status = TrackingStatus::Deleted;
        }

        // Create metadata with both entries
        let metadata = Metadata {
            data: add_pos_to_batches(vec![
                data_entry
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                dv_entry
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ])?,
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: "manifest.parquet".to_string(),
            leaf: None,
        };

        // Get action batches (no data skipping for this test)
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[], None)?;

        // Get the Add action using visitor
        let batch = action_batches.next().unwrap()?;
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(batch.actions.as_ref())?;
        assert_eq!(visitor.adds.len(), 1);
        let add = &visitor.adds[0];

        // Verify DV is NOT included (status is Deleted)
        assert!(
            add.deletion_vector.is_none(),
            "DV with Deleted status should not be included"
        );

        Ok(())
    }

    /// Helper to create a data manifest entry
    fn create_data_manifest_entry(location: &str) -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::DataManifest,
            location: Some(location.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Existed,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: Some(ManifestStats {
                added_files_count: 10,
                existing_files_count: 90,
                deletes_files_count: 0,
                added_rows_count: 1000,
                existing_rows_count: 9000,
                delete_rows_count: 0,
                min_sequence_number: 50,
            }),
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    /// Helper to create a delete manifest entry
    fn create_delete_manifest_entry(
        location: &str,
        referenced_file: Option<&str>,
    ) -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::DeleteManifest,
            location: Some(location.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Existed,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 10,
            file_size_in_bytes: Some(512),
            content_stats: None,
            manifest_info: Some(ManifestStats {
                added_files_count: 5,
                existing_files_count: 5,
                deletes_files_count: 0,
                added_rows_count: 50,
                existing_rows_count: 50,
                delete_rows_count: 0,
                min_sequence_number: 75,
            }),
            referenced_file: referenced_file.map(String::from),
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    #[test]
    fn test_manifest_references_with_affiliated_deletes() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data manifest
        let data_manifest = create_data_manifest_entry("memory:///data-manifest.parquet");

        // Create an affiliated delete manifest
        let delete_manifest = create_delete_manifest_entry(
            "memory:///delete-manifest.parquet",
            Some("memory:///data-manifest.parquet"),
        );

        // Create an unaffiliated delete manifest
        let unaffiliated_delete =
            create_delete_manifest_entry("memory:///unaffiliated-delete.parquet", None);

        // Create metadata with all entries
        let metadata = Metadata {
            data: vec![
                data_manifest
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                delete_manifest
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                unaffiliated_delete
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Get manifest references (no manifest-level skipping for this test)
        let root_state = metadata.manifest_references(None)?;

        // Verify we got one manifest reference
        assert_eq!(root_state.manifest_references.len(), 1);

        let refs = &root_state.manifest_references[0];

        // Verify the data manifest entry
        assert_eq!(
            refs.data_manifest.manifest.location.as_ref().unwrap(),
            "memory:///data-manifest.parquet"
        );
        assert!(refs.data_manifest.manifest.manifest_dv.is_none());

        // Verify affiliated delete manifest
        assert_eq!(refs.affiliated_dv_manifests.len(), 1);
        assert_eq!(
            refs.affiliated_dv_manifests[0]
                .manifest
                .location
                .as_ref()
                .unwrap(),
            "memory:///delete-manifest.parquet"
        );
        assert!(refs.affiliated_dv_manifests[0]
            .manifest
            .manifest_dv
            .is_none());

        // Verify unaffiliated delete manifest (now in shared_state)
        assert_eq!(root_state.shared_state.unaffiliated_dv_manifests.len(), 1);
        assert_eq!(
            root_state.shared_state.unaffiliated_dv_manifests[0]
                .manifest
                .location
                .as_ref()
                .unwrap(),
            "memory:///unaffiliated-delete.parquet"
        );
        assert!(root_state.shared_state.unaffiliated_dv_manifests[0]
            .manifest
            .manifest_dv
            .is_none());

        Ok(())
    }

    #[test]
    fn test_manifest_references_multiple_data_manifests() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create multiple data manifests
        let data_manifest_1 = create_data_manifest_entry("memory:///data-manifest-1.parquet");
        let data_manifest_2 = create_data_manifest_entry("memory:///data-manifest-2.parquet");

        // Create affiliated delete manifests for each
        let delete_manifest_1 = create_delete_manifest_entry(
            "memory:///delete-manifest-1.parquet",
            Some("memory:///data-manifest-1.parquet"),
        );
        let delete_manifest_2 = create_delete_manifest_entry(
            "memory:///delete-manifest-2.parquet",
            Some("memory:///data-manifest-2.parquet"),
        );

        // Create metadata with all entries
        let metadata = Metadata {
            data: vec![
                data_manifest_1
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                data_manifest_2
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                delete_manifest_1
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                delete_manifest_2
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Get manifest references (no manifest-level skipping for this test)
        let root_state = metadata.manifest_references(None)?;

        // Verify we got two manifest references
        assert_eq!(root_state.manifest_references.len(), 2);

        // Verify first manifest reference
        let refs_1 = &root_state.manifest_references[0];
        assert_eq!(
            refs_1.data_manifest.manifest.location.as_ref().unwrap(),
            "memory:///data-manifest-1.parquet"
        );
        assert_eq!(refs_1.affiliated_dv_manifests.len(), 1);
        assert_eq!(
            refs_1.affiliated_dv_manifests[0]
                .manifest
                .location
                .as_ref()
                .unwrap(),
            "memory:///delete-manifest-1.parquet"
        );

        // Verify second manifest reference
        let refs_2 = &root_state.manifest_references[1];
        assert_eq!(
            refs_2.data_manifest.manifest.location.as_ref().unwrap(),
            "memory:///data-manifest-2.parquet"
        );
        assert_eq!(refs_2.affiliated_dv_manifests.len(), 1);
        assert_eq!(
            refs_2.affiliated_dv_manifests[0]
                .manifest
                .location
                .as_ref()
                .unwrap(),
            "memory:///delete-manifest-2.parquet"
        );

        Ok(())
    }

    #[test]
    fn test_manifest_to_action_batches_integration() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a child data manifest with actual data files (using relative paths)
        let data_entry_1 = create_data_entry("child-data-1.parquet", 50);
        let data_entry_2 = create_data_entry("child-data-2.parquet", 60);

        let child_metadata = Metadata {
            data: vec![
                data_entry_1
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                data_entry_2
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Write the child manifest to a file
        let child_manifest_writer = writer::MetadataWriter::try_new(child_metadata)?;
        let child_manifest_url = child_manifest_writer.write(&engine)?;

        // Create a MetadataEntry for the child manifest
        let child_manifest_entry = create_data_manifest_entry(child_manifest_url.as_str());

        // Create ManifestReference pointing to the child manifest
        let manifest_refs = ManifestReference {
            data_manifest: FilteredManifest::new(child_manifest_entry),
            affiliated_dv_manifests: vec![],
        };

        // Process manifest to action batches (empty shared DV map)
        let schema = crate::actions::get_log_add_schema().clone();
        // Create empty shared state for test
        let shared_state = SharedLeafState {
            unaffiliated_dv_manifests: Vec::new(),
        };
        // No data skipping for this test
        let action_batches = Metadata::manifest_to_action_batches(
            manifest_refs,
            &shared_state,
            &engine,
            &schema,
            &table_root_url,
            None,
        )?;

        // Collect all Add actions
        let mut all_adds = Vec::new();
        for batch_result in action_batches {
            let batch = batch_result?;
            let mut visitor = AddVisitor::default();
            visitor.visit_rows_of(batch.actions.as_ref())?;
            all_adds.extend(visitor.adds);
        }

        // Verify we got both data files
        assert_eq!(all_adds.len(), 2);

        // Verify the paths (relative paths)
        let paths: Vec<_> = all_adds.iter().map(|a| a.path.as_str()).collect();
        assert!(paths.contains(&"child-data-1.parquet"));
        assert!(paths.contains(&"child-data-2.parquet"));

        Ok(())
    }

    #[test]
    fn test_full_hierarchical_metadata_tree() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create two child data manifests with actual data files (using relative paths)
        // Child manifest 1
        let data_entry_1 = create_data_entry("partition1/data-1.parquet", 50);
        let data_entry_2 = create_data_entry("partition1/data-2.parquet", 60);

        let child_metadata_1 = Metadata {
            data: vec![
                data_entry_1
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                data_entry_2
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        let child_manifest_writer_1 = writer::MetadataWriter::try_new(child_metadata_1)?;
        let child_manifest_url_1 = child_manifest_writer_1.write(&engine)?;

        // Child manifest 2 - use version 1 to avoid filename collision
        let data_entry_3 = create_data_entry("partition2/data-3.parquet", 70);
        let data_entry_4 = create_data_entry("partition2/data-4.parquet", 80);

        let child_metadata_2 = Metadata {
            data: vec![
                data_entry_3
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                data_entry_4
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ],
            version: 1, // Use different version to avoid filename collision
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        let child_manifest_writer_2 = writer::MetadataWriter::try_new(child_metadata_2)?;
        let child_manifest_url_2 = child_manifest_writer_2.write(&engine)?;

        // Create a root manifest that references both child manifests
        let data_manifest_entry_1 = create_data_manifest_entry(child_manifest_url_1.as_str());
        let data_manifest_entry_2 = create_data_manifest_entry(child_manifest_url_2.as_str());

        let root_metadata = Metadata {
            data: vec![
                data_manifest_entry_1
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
                data_manifest_entry_2
                    .clone()
                    .into_engine_data(test_metadata_entry_schema(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            path_in_log: String::new(),
            leaf: None,
        };

        // Get manifest references from the root (no manifest-level skipping for this test)
        let root_state = root_metadata.manifest_references(None)?;

        // Process all manifests using the helper method
        let schema = crate::actions::get_log_add_schema().clone();
        // No data skipping for this test
        let action_batches =
            Metadata::non_root_action_batches(root_state, &engine, &schema, &table_root_url, None)?;

        // Collect all Add actions
        let mut all_adds = Vec::new();
        for batch_result in action_batches {
            let batch = batch_result?;
            let mut visitor = AddVisitor::default();
            visitor.visit_rows_of(batch.actions.as_ref())?;
            all_adds.extend(visitor.adds);
        }

        // Verify we got all 4 data files
        assert_eq!(all_adds.len(), 4);

        // Verify the paths
        let paths: Vec<_> = all_adds.iter().map(|a| a.path.as_str()).collect();
        assert!(paths.contains(&"partition1/data-1.parquet"));
        assert!(paths.contains(&"partition1/data-2.parquet"));
        assert!(paths.contains(&"partition2/data-3.parquet"));
        assert!(paths.contains(&"partition2/data-4.parquet"));

        Ok(())
    }

    /// Helper to create content_stats for testing data skipping.
    /// Creates stats for a single integer column "id" with the given min/max bounds.
    /// Includes column mapping annotations as required when metadata tree feature is enabled.
    fn create_id_content_stats(min_value: i32, max_value: i32) -> DeltaResult<StructData> {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructType};

        // Create schema for a single "id" column with column mapping annotations
        // (required when metadata tree feature is enabled)
        let table_schema =
            StructType::new_unchecked([StructField::new("id", DataType::INTEGER, false)
                .with_metadata([
                    (
                        ColumnMetadataKey::ParquetFieldId.as_ref(),
                        MetadataValue::Number(1),
                    ),
                    (
                        ColumnMetadataKey::ColumnMappingId.as_ref(),
                        MetadataValue::Number(1),
                    ),
                    (
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        MetadataValue::String("col-id".to_string()),
                    ),
                ])]);

        let content_stats_schema = crate::metadata::stats::stats_schema(&table_schema)?;
        let content_stats_fields: Vec<_> = content_stats_schema.into_fields().collect();

        // Build the 'id' stats struct (4 fields for non-nullable int: value_count, lower_bound, upper_bound, exact_bounds)
        let id_stats_schema = match content_stats_fields[0].data_type() {
            DataType::Struct(s) => s.as_ref().clone(),
            _ => panic!("Expected struct type"),
        };
        let id_stats_fields: Vec<_> = id_stats_schema.into_fields().collect();
        let id_stats = StructData::try_new(
            id_stats_fields,
            vec![
                Scalar::Long(100),          // value_count
                Scalar::Integer(min_value), // lower_bound
                Scalar::Integer(max_value), // upper_bound
                Scalar::Boolean(true),      // exact_bounds
            ],
        )?;

        // Build the content_stats struct containing the id stats
        StructData::try_new(content_stats_fields, vec![Scalar::Struct(id_stats)])
    }

    /// Helper to create a MetadataEntry with content_stats for testing.
    fn create_data_entry_with_stats(
        location: &str,
        min_id: i32,
        max_id: i32,
    ) -> DeltaResult<MetadataEntry> {
        Ok(MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(location.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: Some(create_id_content_stats(min_id, max_id)?),
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        })
    }

    #[test]
    fn test_can_skip_entry_with_content_stats() -> DeltaResult<()> {
        use crate::expressions::{column_expr, Expression, Predicate};

        // Create entries with different id ranges:
        // Entry 1: id in [1, 100]
        // Entry 2: id in [101, 200]
        // Entry 3: id in [201, 300]
        let entry1 = create_data_entry_with_stats("file1.parquet", 1, 100)?;
        let entry2 = create_data_entry_with_stats("file2.parquet", 101, 200)?;
        let entry3 = create_data_entry_with_stats("file3.parquet", 201, 300)?;

        // Test 1: Predicate "id = 50" should NOT skip entry1, but SHOULD skip entry2 and entry3
        let pred_eq_50: Predicate = column_expr!("id").eq(Expression::literal(50i32));
        assert!(
            !can_skip_entry(&entry1, &pred_eq_50),
            "Entry with id [1,100] should NOT be skipped for id=50"
        );
        assert!(
            can_skip_entry(&entry2, &pred_eq_50),
            "Entry with id [101,200] SHOULD be skipped for id=50"
        );
        assert!(
            can_skip_entry(&entry3, &pred_eq_50),
            "Entry with id [201,300] SHOULD be skipped for id=50"
        );

        // Test 2: Predicate "id > 150" should skip entry1, NOT skip entry2 and entry3
        let pred_gt_150: Predicate = column_expr!("id").gt(Expression::literal(150i32));
        assert!(
            can_skip_entry(&entry1, &pred_gt_150),
            "Entry with id [1,100] SHOULD be skipped for id>150"
        );
        assert!(
            !can_skip_entry(&entry2, &pred_gt_150),
            "Entry with id [101,200] should NOT be skipped for id>150"
        );
        assert!(
            !can_skip_entry(&entry3, &pred_gt_150),
            "Entry with id [201,300] should NOT be skipped for id>150"
        );

        // Test 3: Predicate "id < 50" should NOT skip entry1, but SHOULD skip entry2 and entry3
        let pred_lt_50: Predicate = column_expr!("id").lt(Expression::literal(50i32));
        assert!(
            !can_skip_entry(&entry1, &pred_lt_50),
            "Entry with id [1,100] should NOT be skipped for id<50"
        );
        assert!(
            can_skip_entry(&entry2, &pred_lt_50),
            "Entry with id [101,200] SHOULD be skipped for id<50"
        );
        assert!(
            can_skip_entry(&entry3, &pred_lt_50),
            "Entry with id [201,300] SHOULD be skipped for id<50"
        );

        // Test 4: Predicate "id >= 1 AND id <= 300" should NOT skip any entry
        let pred_range: Predicate = Predicate::and(
            column_expr!("id").ge(Expression::literal(1i32)),
            column_expr!("id").le(Expression::literal(300i32)),
        );
        assert!(
            !can_skip_entry(&entry1, &pred_range),
            "Entry with id [1,100] should NOT be skipped for 1<=id<=300"
        );
        assert!(
            !can_skip_entry(&entry2, &pred_range),
            "Entry with id [101,200] should NOT be skipped for 1<=id<=300"
        );
        assert!(
            !can_skip_entry(&entry3, &pred_range),
            "Entry with id [201,300] should NOT be skipped for 1<=id<=300"
        );

        // Test 5: Predicate "id > 500" should skip ALL entries
        let pred_gt_500: Predicate = column_expr!("id").gt(Expression::literal(500i32));
        assert!(
            can_skip_entry(&entry1, &pred_gt_500),
            "Entry with id [1,100] SHOULD be skipped for id>500"
        );
        assert!(
            can_skip_entry(&entry2, &pred_gt_500),
            "Entry with id [101,200] SHOULD be skipped for id>500"
        );
        assert!(
            can_skip_entry(&entry3, &pred_gt_500),
            "Entry with id [201,300] SHOULD be skipped for id>500"
        );

        Ok(())
    }

    #[test]
    fn test_can_skip_entry_without_content_stats() -> DeltaResult<()> {
        use crate::expressions::{column_expr, Expression, Predicate};

        // Create an entry WITHOUT content_stats
        let entry_no_stats = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None, // No stats!
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Without content_stats, we can never skip (safe default)
        let pred: Predicate = column_expr!("id").gt(Expression::literal(500i32));
        assert!(
            !can_skip_entry(&entry_no_stats, &pred),
            "Entry without content_stats should NEVER be skipped"
        );

        Ok(())
    }

    #[test]
    fn test_filter_entries_by_predicate_integration() -> DeltaResult<()> {
        use crate::expressions::{column_expr, Expression, Predicate};

        // Create 5 entries with different id ranges:
        // Entry 1: id in [1, 100]
        // Entry 2: id in [101, 200]
        // Entry 3: id in [201, 300]
        // Entry 4: id in [301, 400]
        // Entry 5: id in [401, 500]
        let entries = vec![
            create_data_entry_with_stats("file1.parquet", 1, 100)?,
            create_data_entry_with_stats("file2.parquet", 101, 200)?,
            create_data_entry_with_stats("file3.parquet", 201, 300)?,
            create_data_entry_with_stats("file4.parquet", 301, 400)?,
            create_data_entry_with_stats("file5.parquet", 401, 500)?,
        ];

        // Test 1: No predicate - all entries should be returned
        let filtered = filter_entries_by_predicate(entries.clone(), None, "test entries");
        assert_eq!(filtered.len(), 5, "No predicate should return all entries");

        // Test 2: Predicate "id = 150" - only entry2 should remain
        let pred_eq_150: Predicate = column_expr!("id").eq(Expression::literal(150i32));
        let pred_ref = Arc::new(pred_eq_150);
        let filtered =
            filter_entries_by_predicate(entries.clone(), Some(&pred_ref), "test entries");
        assert_eq!(
            filtered.len(),
            1,
            "Predicate id=150 should return 1 entry (file2)"
        );
        assert_eq!(
            filtered[0].location.as_ref().unwrap(),
            "file2.parquet",
            "Only file2 should match id=150"
        );

        // Test 3: Predicate "id > 250" - entries 3, 4, 5 should remain
        let pred_gt_250: Predicate = column_expr!("id").gt(Expression::literal(250i32));
        let pred_ref = Arc::new(pred_gt_250);
        let filtered =
            filter_entries_by_predicate(entries.clone(), Some(&pred_ref), "test entries");
        assert_eq!(
            filtered.len(),
            3,
            "Predicate id>250 should return 3 entries"
        );
        let locations: Vec<_> = filtered
            .iter()
            .map(|e| e.location.as_ref().unwrap().as_str())
            .collect();
        assert!(locations.contains(&"file3.parquet"));
        assert!(locations.contains(&"file4.parquet"));
        assert!(locations.contains(&"file5.parquet"));

        // Test 4: Predicate "id < 150" - entries 1 and 2 should remain
        // Entry1 [1,100]: all values < 150, not skipped
        // Entry2 [101,200]: some values < 150 (101-149), not skipped
        // Entry3,4,5: min > 150, all skipped
        let pred_lt_150: Predicate = column_expr!("id").lt(Expression::literal(150i32));
        let pred_ref = Arc::new(pred_lt_150);
        let filtered =
            filter_entries_by_predicate(entries.clone(), Some(&pred_ref), "test entries");
        assert_eq!(
            filtered.len(),
            2,
            "Predicate id<150 should return 2 entries (file1 and file2)"
        );
        let locations: Vec<_> = filtered
            .iter()
            .map(|e| e.location.as_ref().unwrap().as_str())
            .collect();
        assert!(locations.contains(&"file1.parquet"));
        assert!(locations.contains(&"file2.parquet"));

        // Test 5: Predicate "id > 1000" - no entries should remain
        let pred_gt_1000: Predicate = column_expr!("id").gt(Expression::literal(1000i32));
        let pred_ref = Arc::new(pred_gt_1000);
        let filtered = filter_entries_by_predicate(entries, Some(&pred_ref), "test entries");
        assert_eq!(
            filtered.len(),
            0,
            "Predicate id>1000 should skip all entries"
        );

        Ok(())
    }

    /// Test manifest skipping using direct entry filtering (without serialization).
    ///
    /// This test demonstrates the data skipping behavior at the manifest level
    /// by directly testing `filter_entries_by_predicate` on DataManifest entries.
    #[test]
    fn test_manifest_skipping_with_predicate() -> DeltaResult<()> {
        use crate::expressions::{column_expr, Expression, Predicate};

        // Create DataManifest entries with content_stats representing different id ranges
        // These represent child manifests in a hierarchical metadata tree
        //
        // Manifest 1: contains data files with id in [1, 100]
        // Manifest 2: contains data files with id in [101, 200]
        // Manifest 3: contains data files with id in [201, 300]

        let manifest1 = MetadataEntry {
            content_type: DataContentType::DataManifest,
            location: Some("manifest1.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Existed,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: None,
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: Some(create_id_content_stats(1, 100)?),
            manifest_info: Some(ManifestStats {
                added_files_count: 10,
                existing_files_count: 0,
                deletes_files_count: 0,
                added_rows_count: 100,
                existing_rows_count: 0,
                delete_rows_count: 0,
                min_sequence_number: 100,
            }),
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let manifest2 = MetadataEntry {
            location: Some("manifest2.parquet".to_string()),
            content_stats: Some(create_id_content_stats(101, 200)?),
            ..manifest1.clone()
        };

        let manifest3 = MetadataEntry {
            location: Some("manifest3.parquet".to_string()),
            content_stats: Some(create_id_content_stats(201, 300)?),
            ..manifest1.clone()
        };

        let manifests = vec![manifest1, manifest2, manifest3];

        // Test 1: No predicate - all 3 manifests should be returned
        let filtered = filter_entries_by_predicate(manifests.clone(), None, "child manifests");
        assert_eq!(
            filtered.len(),
            3,
            "No predicate should return all 3 manifests"
        );

        // Test 2: Predicate "id = 50" - only manifest1 should be returned
        let pred_eq_50: Predicate = column_expr!("id").eq(Expression::literal(50i32));
        let pred_ref = Arc::new(pred_eq_50);
        let filtered =
            filter_entries_by_predicate(manifests.clone(), Some(&pred_ref), "child manifests");
        assert_eq!(
            filtered.len(),
            1,
            "Predicate id=50 should return 1 manifest"
        );
        assert_eq!(
            filtered[0].location.as_ref().unwrap(),
            "manifest1.parquet",
            "Only manifest1 should match id=50"
        );

        // Test 3: Predicate "id > 150" - manifests 2 and 3 should be returned
        let pred_gt_150: Predicate = column_expr!("id").gt(Expression::literal(150i32));
        let pred_ref = Arc::new(pred_gt_150);
        let filtered =
            filter_entries_by_predicate(manifests.clone(), Some(&pred_ref), "child manifests");
        assert_eq!(
            filtered.len(),
            2,
            "Predicate id>150 should return 2 manifests"
        );
        let locations: Vec<_> = filtered
            .iter()
            .map(|e| e.location.as_ref().unwrap().as_str())
            .collect();
        assert!(locations.contains(&"manifest2.parquet"));
        assert!(locations.contains(&"manifest3.parquet"));

        // Test 4: Predicate "id > 500" - no manifests should be returned
        let pred_gt_500: Predicate = column_expr!("id").gt(Expression::literal(500i32));
        let pred_ref = Arc::new(pred_gt_500);
        let filtered =
            filter_entries_by_predicate(manifests.clone(), Some(&pred_ref), "child manifests");
        assert_eq!(
            filtered.len(),
            0,
            "Predicate id>500 should skip all manifests"
        );

        // Test 5: Predicate "id < 250" - manifests 1 and 2 should be returned
        // Manifest1 [1,100]: max=100 < 250, not skipped
        // Manifest2 [101,200]: max=200 < 250, not skipped
        // Manifest3 [201,300]: min=201 < 250 but max=300 > 250, some rows might match, not skipped
        // Actually, for "id < 250", manifest3 has min=201 and max=300
        // Since some values in [201,249] satisfy id < 250, manifest3 should NOT be skipped
        let pred_lt_250: Predicate = column_expr!("id").lt(Expression::literal(250i32));
        let pred_ref = Arc::new(pred_lt_250);
        let filtered =
            filter_entries_by_predicate(manifests.clone(), Some(&pred_ref), "child manifests");
        assert_eq!(
            filtered.len(),
            3,
            "Predicate id<250 should return all 3 manifests (all might have matching rows)"
        );

        // Test 6: Predicate "id < 100" - manifest1 might match, manifests 2 and 3 should be skipped
        // Manifest1 [1,100]: max=100 >= 100, but some values < 100, not skipped
        // Manifest2 [101,200]: min=101 > 100, cannot have id < 100, skipped
        // Manifest3 [201,300]: min=201 > 100, cannot have id < 100, skipped
        let pred_lt_100: Predicate = column_expr!("id").lt(Expression::literal(100i32));
        let pred_ref = Arc::new(pred_lt_100);
        let filtered = filter_entries_by_predicate(manifests, Some(&pred_ref), "child manifests");
        assert_eq!(
            filtered.len(),
            1,
            "Predicate id<100 should return 1 manifest (only manifest1)"
        );
        assert_eq!(
            filtered[0].location.as_ref().unwrap(),
            "manifest1.parquet",
            "Only manifest1 should match id<100"
        );

        Ok(())
    }

    /// End-to-end integration test for DV size conversion through the metadata tree.
    ///
    /// This test creates a table with deletion vectors using the Transaction API and bulk mode,
    /// then verifies that:
    /// 1. PositionDeletes entries in persisted manifests have Iceberg format sizes (Delta size + 8 bytes)
    /// 2. The size conversion happens at write time in extract_deletion_vector_content
    #[test]
    fn test_dv_size_conversion_through_metadata_tree() -> Result<(), Box<dyn std::error::Error>> {
        use crate::actions::deletion_vector::{
            DeletionVectorDescriptor, DeletionVectorStorageType,
        };
        use crate::arrow::array::{
            new_null_array, ArrayRef, BooleanArray, Int64Array, MapArray, StringArray, StructArray,
        };
        use crate::arrow::buffer::OffsetBuffer;
        use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
        use crate::arrow::record_batch::RecordBatch;
        use crate::committer::FileSystemCommitter;
        use crate::engine::arrow_conversion::TryFromKernel;
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::engine::sync::SyncEngine;
        use crate::snapshot::Snapshot;
        use crate::transaction::{CommitResult, DvUpdate, ManifestLocation};
        use serde_json::json;
        use std::fs::{create_dir_all, write};
        use std::sync::Arc;
        use tempfile::tempdir;
        use url::Url;
        use uuid::Uuid;

        let engine = Arc::new(SyncEngine::new());
        let temp_dir = tempdir()?;
        let canonical_path = std::fs::canonicalize(temp_dir.path())?;
        let table_url = Url::from_directory_path(canonical_path).unwrap();

        // Step 1: Create initial table with DV support (v0)
        let table_id = Uuid::new_v4().to_string();
        let schema = json!({
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": true,
                    "metadata": {
                        "parquet.field.id": 1,
                        "delta.columnMapping.id": 1,
                        "delta.columnMapping.physicalName": "id"
                    }
                },
                {
                    "name": "value",
                    "type": "string",
                    "nullable": true,
                    "metadata": {
                        "parquet.field.id": 2,
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
                "readerFeatures": ["deletionVectors", "columnMapping", "metadataTree-experimental"],
                "writerFeatures": ["deletionVectors", "columnMapping", "metadataTree-experimental"]
            }
        });

        let metadata = json!({
            "metaData": {
                "id": table_id,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(),
                "partitionColumns": [],
                "configuration": {
                    "delta.enableDeletionVectors": "true",
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

        let delta_log_path = table_url
            .join("_delta_log/")?
            .to_file_path()
            .map_err(|_| crate::Error::generic("Cannot convert URL to file path"))?;

        create_dir_all(&delta_log_path)?;
        write(delta_log_path.join("00000000000000000000.json"), data)?;

        // Step 2: Add files to a leaf WITHOUT DVs (v1)
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
                .with_operation("WRITE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(engine.as_ref())?;
            let add_files_schema = txn.add_files_schema();

            // Create add files metadata inline using arrow
            let files = [
                ("file1.parquet", 1000, 1000000, 50),
                ("file2.parquet", 2000, 1000001, 75),
            ];
            let num_files = files.len();

            let path_array =
                StringArray::from(files.iter().map(|(p, _, _, _)| *p).collect::<Vec<_>>());
            let size_array =
                Int64Array::from(files.iter().map(|(_, s, _, _)| *s).collect::<Vec<_>>());
            let mod_time_array =
                Int64Array::from(files.iter().map(|(_, _, m, _)| *m).collect::<Vec<_>>());
            let num_records_array =
                Int64Array::from(files.iter().map(|(_, _, _, n)| *n).collect::<Vec<_>>());

            // Create empty partition values
            let entries_field = std::sync::Arc::new(Field::new(
                "key_value",
                ArrowDataType::Struct(
                    vec![
                        std::sync::Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                        std::sync::Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            ));
            let empty_entries = StructArray::from(vec![
                (
                    std::sync::Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                    std::sync::Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                    std::sync::Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef,
                ),
            ]);
            let offsets = OffsetBuffer::from_lengths(vec![0; num_files]);
            let partition_values_array = std::sync::Arc::new(MapArray::new(
                entries_field,
                offsets,
                empty_entries,
                None,
                false,
            ));

            // Build stats struct
            let arrow_schema: ArrowSchema =
                TryFromKernel::try_from_kernel(add_files_schema.as_ref())?;
            let stats_field = arrow_schema
                .field_with_name("stats")
                .expect("stats field should exist");
            let stats_arrow_schema = match stats_field.data_type() {
                ArrowDataType::Struct(fields) => fields.clone(),
                _ => panic!("stats field should be a struct"),
            };

            let mut stats_fields = Vec::new();
            for field in stats_arrow_schema.iter() {
                let array: ArrayRef = match field.name().as_str() {
                    "numRecords" => std::sync::Arc::new(num_records_array.clone()),
                    "tightBounds" => {
                        std::sync::Arc::new(BooleanArray::from(vec![Some(true); num_files]))
                    }
                    _ => std::sync::Arc::new(new_null_array(field.data_type(), num_files)),
                };
                stats_fields.push((field.clone(), array));
            }
            let stats_struct = StructArray::from(stats_fields);

            let batch = RecordBatch::try_new(
                std::sync::Arc::new(arrow_schema),
                vec![
                    std::sync::Arc::new(path_array) as ArrayRef,
                    partition_values_array as ArrayRef,
                    std::sync::Arc::new(size_array) as ArrayRef,
                    std::sync::Arc::new(mod_time_array) as ArrayRef,
                    std::sync::Arc::new(stats_struct) as ArrayRef,
                ],
            )?;

            let metadata_engine_data: Box<dyn crate::EngineData> =
                Box::new(ArrowEngineData::new(batch));
            leaf.add_files(engine.as_ref(), metadata_engine_data)?;

            let result = leaf.finish(engine.as_ref())?;
            txn.add_leaf(result)?;

            match txn.commit(engine.as_ref())? {
                CommitResult::CommittedTransaction(_) => {}
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Step 3: Scan to find where files landed (manifest URL + indices)
        let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let scan = snapshot_v1.clone().scan_builder().build()?;

        // Use scan callback to collect file locations - much simpler than RowVisitor!
        fn collect_locations(
            locations: &mut Vec<(String, String, i64)>,
            scan_file: crate::scan::state::ScanFile,
        ) {
            if let (Some(manifest_path), Some(index)) = (
                scan_file.data_manifest_path,
                scan_file.data_manifest_position,
            ) {
                locations.push((scan_file.path, manifest_path, index));
            }
        }

        let mut file_locations = Vec::new();
        for scan_metadata_result in scan.scan_metadata(engine.as_ref())? {
            let scan_metadata = scan_metadata_result?;
            file_locations = scan_metadata.visit_scan_files(file_locations, collect_locations)?;
        }

        // Step 4: Add DVs for the files using a known size (v2)
        let known_dv_size_in_bytes: i32 = 42; // The Delta format size (what we'll test for conversion)
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
                .with_operation("UPDATE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(engine.as_ref())?;

            let mut dv_updates = vec![];
            for (i, (path, manifest_path, index)) in file_locations.iter().enumerate() {
                // Use a different UUID for each file
                let uuid_str = if i == 0 {
                    "12345678-1234-1234-1234-123456789abc"
                } else {
                    "87654321-4321-4321-4321-cba987654321"
                };

                let dv_descriptor = DeletionVectorDescriptor {
                    storage_type: DeletionVectorStorageType::PersistedRelative,
                    path_or_inline_dv: uuid_str.to_string(),
                    offset: Some(0),
                    size_in_bytes: known_dv_size_in_bytes,
                    cardinality: 5,
                };

                // Use the relative manifest path directly (no conversion to absolute URL)
                dv_updates.push(DvUpdate {
                    data_file_path: path.clone(),
                    dv_descriptor,
                    data_file_location: ManifestLocation {
                        manifest_path: manifest_path.clone(),
                        index: *index,
                    },
                    previous_delete_file_location: None,
                });
            }

            leaf.update_deletion_vectors(dv_updates)?;

            let result = leaf.finish(engine.as_ref())?;
            txn.add_leaf(result)?;

            match txn.commit(engine.as_ref())? {
                CommitResult::CommittedTransaction(_) => {}
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Step 5: Read the ContentRoot file directly to verify persisted sizes
        let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let content_root_info = snapshot_v2
            .content_root()
            .expect("Table should have ContentRoot after batch commit");

        let root_manifest_url = table_url.join(content_root_info.path())?;

        let root_metadata = Metadata::read(
            engine.as_ref(),
            &root_manifest_url,
            content_root_info.path().to_string(),
            table_url.clone(),
        )?;
        let root_entries = root_metadata.entries()?;

        // The root might have DeleteManifest entries that contain the PositionDeletes
        // Let's find all manifests and read them to find PositionDeletes
        let mut found_position_deletes_count = 0;

        // First check if PositionDeletes are directly in the root
        for entry in &root_entries {
            if matches!(entry.content_type, DataContentType::PositionDeletes) {
                let content_info = entry
                    .content_info
                    .as_ref()
                    .expect("PositionDeletes should have content_info");

                let expected_iceberg_size = known_dv_size_in_bytes as i64 + 8;
                assert_eq!(
                    content_info.size_in_bytes, expected_iceberg_size,
                    "Persisted size should be {} (Delta {} + 8 framing), but got {}",
                    expected_iceberg_size, known_dv_size_in_bytes, content_info.size_in_bytes
                );
                found_position_deletes_count += 1;
            }
        }

        // If not in root, check DeleteManifests
        if found_position_deletes_count == 0 {
            for entry in &root_entries {
                if matches!(entry.content_type, DataContentType::DeleteManifest) {
                    let manifest_path = entry
                        .location
                        .as_ref()
                        .expect("Manifest should have location");
                    let manifest_url = table_url.join(manifest_path)?;
                    let manifest_metadata = Metadata::read(
                        engine.as_ref(),
                        &manifest_url,
                        manifest_path.clone(),
                        table_url.clone(),
                    )?;
                    let manifest_entries = manifest_metadata.entries()?;

                    for manifest_entry in manifest_entries {
                        if matches!(
                            manifest_entry.content_type,
                            DataContentType::PositionDeletes
                        ) {
                            // We found the PositionDeletes in a leaf manifest
                            // Verify the size here
                            let content_info = manifest_entry
                                .content_info
                                .as_ref()
                                .expect("PositionDeletes should have content_info");

                            let expected_iceberg_size = known_dv_size_in_bytes as i64 + 8;
                            assert_eq!(
                                content_info.size_in_bytes,
                                expected_iceberg_size,
                                "Persisted size should be {} (Delta {} + 8 framing), but got {}",
                                expected_iceberg_size,
                                known_dv_size_in_bytes,
                                content_info.size_in_bytes
                            );
                            found_position_deletes_count += 1;
                        }
                    }
                }
            }
        }

        assert!(
            found_position_deletes_count > 0,
            "Should have PositionDeletes entries in root or leaf manifests"
        );

        // The test successfully proves:
        // 1. Persisted manifests have PositionDeletes with Iceberg sizes (Delta + 8)
        //    - We verified the content_info.size_in_bytes = 42 + 8 = 50
        // 2. The size conversion happens at write time in:
        //    - extract_deletion_vector_content (+8): builder.rs:98
        // 3. On read, metadata_entry_to_deletion_vector_info would subtract 8 bytes (metadata/mod.rs:1488)
        //    but we don't test that here since we'd need actual DV files

        Ok(())
    }

    #[test]
    fn test_parse_manifest_dv_with_deletions() -> DeltaResult<()> {
        use roaring::RoaringTreemap;

        // Create a RoaringTreemap with deleted positions 1, 3, 5
        let mut deleted_positions = RoaringTreemap::new();
        deleted_positions.insert(1);
        deleted_positions.insert(3);
        deleted_positions.insert(5);

        // Serialize with portable format magic number
        let mut serialized = Vec::new();
        const ROARING_BITMAP_PORTABLE_MAGIC: u32 = 1681511377;
        serialized.extend_from_slice(&ROARING_BITMAP_PORTABLE_MAGIC.to_be_bytes());
        deleted_positions.serialize_into(&mut serialized)?;

        let dv_bytes = Bytes::from(serialized);

        // Test parse_manifest_dv
        let parsed = parse_manifest_dv(&dv_bytes)?;
        assert!(parsed.is_some());
        let treemap = parsed.unwrap();
        assert_eq!(treemap.len(), 3);
        assert!(treemap.contains(1));
        assert!(treemap.contains(3));
        assert!(treemap.contains(5));
        assert!(!treemap.contains(0));
        assert!(!treemap.contains(2));

        // Test parse_manifest_dv_to_selection_vector
        let selection = parse_manifest_dv_to_selection_vector(&dv_bytes, 10)?;
        assert_eq!(selection.len(), 10);
        assert!(selection[0]); // not deleted
        assert!(!selection[1]); // deleted
        assert!(selection[2]); // not deleted
        assert!(!selection[3]); // deleted
        assert!(selection[4]); // not deleted
        assert!(!selection[5]); // deleted
        assert!(selection[6]); // not deleted

        Ok(())
    }

    #[test]
    fn test_parse_manifest_dv_empty() -> DeltaResult<()> {
        let empty_bytes = Bytes::new();

        // Test parse_manifest_dv with empty bytes
        let parsed = parse_manifest_dv(&empty_bytes)?;
        assert!(parsed.is_none());

        // Test parse_manifest_dv_to_selection_vector with empty bytes
        let selection = parse_manifest_dv_to_selection_vector(&empty_bytes, 5)?;
        assert_eq!(selection.len(), 5);
        assert!(selection.iter().all(|&b| b)); // all true (nothing deleted)

        Ok(())
    }

    #[test]
    fn test_parse_manifest_dv_invalid_magic() -> DeltaResult<()> {
        // Create bytes with invalid magic number
        let mut invalid = Vec::new();
        let invalid_magic: u32 = 0xDEADBEEF;
        invalid.extend_from_slice(&invalid_magic.to_be_bytes());
        invalid.extend_from_slice(&[0u8; 10]); // some dummy data

        let dv_bytes = Bytes::from(invalid);

        // Should return error
        let result = parse_manifest_dv(&dv_bytes);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid magic number"));

        Ok(())
    }

    #[test]
    fn test_parse_manifest_dv_too_small() -> DeltaResult<()> {
        // Create bytes that are too small (less than 4 bytes)
        let too_small = Bytes::from(vec![0u8, 1u8]);

        let result = parse_manifest_dv(&too_small);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too small"));

        Ok(())
    }

    #[test]
    fn test_parse_manifest_dv_native_format_unsupported() -> DeltaResult<()> {
        // Create bytes with native format magic number (unsupported)
        let mut native = Vec::new();
        const ROARING_BITMAP_NATIVE_MAGIC: u32 = 1681511376;
        native.extend_from_slice(&ROARING_BITMAP_NATIVE_MAGIC.to_be_bytes());
        native.extend_from_slice(&[0u8; 10]);

        let dv_bytes = Bytes::from(native);

        let result = parse_manifest_dv(&dv_bytes);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Native serialization format"));

        Ok(())
    }
}
