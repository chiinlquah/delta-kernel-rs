use crate::metadata::Metadata;
use crate::path::ParsedLogPath;
use crate::FilteredEngineData;
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

        // Create an iterator over the metadata data (already in EngineData format)
        let empty_schema = std::sync::Arc::new(crate::schema::StructType::new_unchecked([]));
        let data_iter = self.metadata.data.iter().map(|engine_data| {
            // Hack to get a copy of engine_data which is not otherwise copyable.
            let appended = engine_data.append_columns(empty_schema.clone(), vec![])?;
            Ok(FilteredEngineData::with_all_rows_selected(appended))
        });

        let file_meta = engine
            .parquet_handler()
            .write_parquet_file(path.clone(), Box::new(data_iter))?;

        Ok(file_meta)
    }
}
