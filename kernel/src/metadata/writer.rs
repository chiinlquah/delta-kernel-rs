use crate::metadata::Metadata;
use crate::path::ParsedLogPath;
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
    /// Creates a new [`MetadataWriter`] for given content root metadata.
    pub(crate) fn try_new(metadata: Metadata) -> DeltaResult<Self> {
        Ok(Self { metadata })
    }

    /// Returns the URL where the content metadata file should be written.
    ///
    /// This method generates the checkpoint path based on the table's content root and the version
    /// of the underlying snapshot being checkpointed. The resulting path follows the
    /// Delta checkpoint naming convention (where the version is zero-padded to 20 digits):
    ///
    /// For root manifests (no leaf UUID):
    ///   `<table_root>/_delta_log/<version>.content.parquet`
    ///
    /// For leaf manifests (with leaf UUID):
    ///   `<table_root>/_delta_log/<version>.content.<uuid>.parquet`
    ///
    /// For example, if the table root is `s3://bucket/path` and the version is `10`:
    /// - Root: `s3://bucket/path/_delta_log/00000000000000000010.content.parquet`
    /// - Leaf: `s3://bucket/path/_delta_log/00000000000000000010.content.550e8400-e29b-41d4-a716-446655440000.parquet`
    fn checkpoint_path(&self) -> DeltaResult<Url> {
        ParsedLogPath::new_content_metadata_path(
            &self.metadata.table_root,
            self.metadata.version,
            self.metadata.leaf(),
        )
        .map(|parsed| parsed.location)
    }

    pub(crate) fn write(self, engine: &dyn Engine) -> DeltaResult<Url> {
        let path = self.checkpoint_path()?;
        let data_iter = self.metadata.data.into_iter().map(Ok);

        engine
            .parquet_handler()
            .write_parquet_file(path.clone(), Box::new(data_iter))?;

        Ok(path)
    }
}
