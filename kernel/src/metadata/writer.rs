use crate::metadata::{Metadata, MetadataEntry};
use crate::path::ParsedLogPath;
use crate::schema::ToSchema;
use crate::IntoEngineData;
use crate::{DeltaResult, Engine};
use url::Url;

/// Orchestrates the process of creating a V3 checkpoint for a table.
///
#[allow(dead_code)]
pub(crate) struct MetadataWriter {
    pub(crate) metadata: Metadata,
}

#[allow(dead_code)]
impl MetadataWriter {
    /// Creates a new [`MetadataWriter`] for given metadata.
    pub(crate) fn try_new(metadata: Metadata) -> DeltaResult<Self> {
        Ok(Self { metadata })
    }

    /// Returns the URL where the checkpoint file should be written.
    ///
    /// This method generates the checkpoint path based on the table's root and the version
    /// of the underlying snapshot being checkpointed. The resulting path follows the classic
    /// Delta checkpoint naming convention (where the version is zero-padded to 20 digits):
    ///
    /// `<table_root>/<version>.checkpoint.parquet`
    ///
    /// For example, if the table root is `s3://bucket/path` and the version is `10`,
    /// the checkpoint path will be: `s3://bucket/path/00000000000000000010.checkpoint.parquet`
    fn checkpoint_path(&self) -> DeltaResult<Url> {
        // TODO: We need to create unique paths for root/leaves
        // I think the root should follow the path, but it would be nice to
        // start writing and have the ability to push down the root to a leave
        // Needs discussion with a broader audience to discuss the Delta limitations
        ParsedLogPath::new_classic_parquet_checkpoint(
            &self.metadata.table_root,
            self.metadata.version.unwrap_or(0),
        )
        .map(|parsed| parsed.location)
    }

    // TODO: Decide on the ReturnType, I think we should go with some kind of CommitResult
    pub(crate) fn write(&self, engine: &dyn Engine) -> DeltaResult<Url> {
        let path = self.checkpoint_path()?;

        // Create an iterator that converts each metadata entry to FilteredEngineData
        let data_iter = self.metadata.entries.iter().map(|entry| {
            let engine_data = entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), engine)?;
            Ok(engine_data.into())
        });

        engine
            .parquet_handler()
            .write_parquet_file(path.clone(), Box::new(data_iter))?;

        Ok(path)
    }
}
