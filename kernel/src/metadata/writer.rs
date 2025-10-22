use crate::metadata::{Metadata, MetadataEntry};
use crate::path::ParsedLogPath;
use crate::schema::ToSchema;
use crate::IntoEngineData;
use crate::{DeltaResult, Engine, FileMeta};
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

    /// Returns the URL where the content metadata root file should be written.
    ///
    /// This method generates the checkpoint path based on the table's content root and the version
    /// of the underlying snapshot being checkpointed. The resulting path follows the classic
    /// Delta checkpoint naming convention (where the version is zero-padded to 20 digits):
    ///
    /// `<table_root>/<version>.content.parquet`
    ///
    /// For example, if the table root is `s3://bucket/path` and the version is `10`,
    /// the checkpoint path will be: `s3://bucket/path/00000000000000000010.content.parquet`
    fn checkpoint_path(&self) -> DeltaResult<Url> {
        ParsedLogPath::new_content_metadata_path(&self.metadata.table_root, self.metadata.version)
            .map(|parsed| parsed.location)
    }

    pub(crate) fn write(&self, engine: &dyn Engine) -> DeltaResult<FileMeta> {
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

        // Try to get the file size from the file system
        let size = path
            .to_file_path()
            .ok()
            .and_then(|file_path| std::fs::metadata(file_path).ok())
            .map(|metadata| metadata.len())
            .unwrap_or(0);

        // Create FileMeta with size from file system or 0, and last_modified as 0
        Ok(FileMeta {
            location: path,
            last_modified: 0,
            size,
        })
    }
}
