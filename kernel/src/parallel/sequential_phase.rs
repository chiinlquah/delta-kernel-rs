//! Sequential log replay processor that happens before the parallel phase.
//!
//! This module provides sequential phase log replay that processes commits and
//! single-part checkpoint manifests, then returns the processor and any files (sidecars or
//! multi-part checkpoint parts) for parallel processing by the parallel phase. This phase
//! must be completed before the parallel phase can start.
//!
//! For multi-part checkpoints, the sequential phase skips manifest processing and returns
//! the checkpoint parts for parallel processing.

use std::sync::Arc;

use itertools::Itertools;

use crate::actions::get_commit_schema;
use crate::log_reader::checkpoint_manifest::CheckpointManifestReader;
use crate::log_reader::commit::CommitReader;
use crate::log_replay::LogReplayProcessor;
use crate::log_segment::LogSegment;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, FileMeta};

/// Sequential log replay processor for parallel execution.
///
/// This iterator processes log replay sequentially:
/// 1. Commit files (JSON)
/// 2. Manifest (single-part checkpoint, if present)
///
/// After exhaustion, call `finish()` to extract:
/// - The processor (for serialization and distribution)
/// - Files (sidecars or multi-part checkpoint parts) for parallel processing
///
/// # Type Parameters
/// - `P`: A [`LogReplayProcessor`] implementation that processes action batches
///
/// # Example
///
/// ```ignore
/// let mut sequential = SequentialPhase::try_new(processor, log_segment, engine)?;
///
/// // Iterate over sequential batches
/// for batch in sequential.by_ref() {
///     let metadata = batch?;
///     // Process metadata
/// }
///
/// // Extract processor and tasks for distribution (if needed)
/// match sequential.finish(engine.as_ref())? {
///     AfterSequential::Parallel { processor, tasks } => {
///         // Parallel phase needed - distribute tasks across workers.
///         // The processor now contains any shared metadata state internally.
///         // If crossing the network boundary, the processor must be serialized.
///         let serialized_processor = serialize(&processor)?;
///         match tasks {
///             PartitionTask::CheckpointFiles(files) => {
///                 let partitions = partition_files(files, num_workers);
///                 for (worker, partition) in partitions {
///                     worker.send(serialized_processor.clone(), partition)?;
///                 }
///             }
///             PartitionTask::MetadataManifests(manifests) => {
///                 // Processor contains shared metadata state internally
///                 // Each worker will build the shared DV map once and process manifests
///                 let partitions = partition_manifests(manifests, num_workers);
///                 for (worker, partition) in partitions {
///                     worker.send(serialized_processor.clone(), partition)?;
///                 }
///             }
///         }
///     }
///     AfterSequential::Done(processor) => {
///         // No parallel phase needed - all processing complete sequentially
///         println!("Log replay complete");
///     }
/// }
/// ```
#[allow(unused)]
pub(crate) struct SequentialPhase<P: LogReplayProcessor> {
    // The processor that will be used to process the action batches
    processor: P,
    // The commit reader that will be used to read the commit files
    commit_phase: Option<CommitReader>,
    // The checkpoint manifest reader that will be used to read the checkpoint manifest files.
    // If the checkpoint is single-part, this will be Some(CheckpointManifestReader).
    checkpoint_manifest_phase: Option<CheckpointManifestReader>,
    // Whether the iterator has been fully exhausted
    is_finished: bool,
    // Checkpoint parts for potential parallel phase processing
    checkpoint_parts: Vec<FileMeta>,
    // data from content root.
    content_tree_leaf_state: Option<crate::metadata::LeafReferences>,
    content_root_phase:
        Option<Box<dyn Iterator<Item = DeltaResult<crate::log_replay::ActionsBatch>> + Send>>,
}

/// Tasks that can be partitioned and processed in parallel.
#[allow(unused)]
pub(crate) enum PartitionTasks {
    /// Checkpoint files (sidecars or multi-part checkpoint parts) to be read and processed.
    CheckpointFiles(Vec<FileMeta>),
    /// Metadata tree manifests to be read and processed.
    ///
    /// Each manifest reference should be distributed to a worker for parallel processing.
    /// Workers must use the `shared_metadata_state` from AfterSequential::Parallel to build
    /// the shared DV map before processing manifests.
    MetadataManifests(Vec<crate::metadata::ManifestReference>),
}

/// Result of sequential log replay processing.
#[allow(unused)]
pub(crate) enum AfterSequential<P: LogReplayProcessor> {
    /// All processing complete sequentially - no parallel phase needed.
    Done(P),
    /// Parallel phase needed - distribute tasks for parallel processing.
    ///
    /// # Fields
    /// - `processor`: The log replay processor containing all state for parallel execution,
    ///   including any shared metadata state from the metadata tree root.
    /// - `tasks`: The tasks to partition across workers (checkpoint files or metadata manifests)
    Parallel { processor: P, tasks: PartitionTasks },
}

impl<P: LogReplayProcessor> SequentialPhase<P> {
    /// Create a new sequential phase log replay.
    ///
    /// # Parameters
    /// - `processor`: The log replay processor
    /// - `log_segment`: The log segment to process
    /// - `engine`: Engine for reading files
    #[allow(unused)]
    pub(crate) fn try_new(
        processor: P,
        log_segment: &LogSegment,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let commit_phase = Some(CommitReader::try_new(
            engine.as_ref(),
            log_segment,
            get_commit_schema().clone(),
            None,
        )?);

        // Concurrently start reading the checkpoint manifest. Only create a checkpoint manifest
        // reader if the checkpoint is single-part.
        let checkpoint_manifest_phase = match log_segment.checkpoint_parts.as_slice() {
            [single_part] => Some(CheckpointManifestReader::try_new(
                engine.clone(),
                single_part,
                log_segment.log_root.clone(),
            )?),
            _ => None,
        };

        let checkpoint_parts = log_segment
            .checkpoint_parts
            .iter()
            .map(|path| path.location.clone())
            .collect_vec();

        // Check for ContentRoot action in the log segment
        let content_root = log_segment
            .content_root_with_version(engine.as_ref())?
            .map(|(cr, _)| cr);
        let metadata = content_root
            .map(|cr| {
                crate::metadata::Metadata::new_from_content_root(
                    engine.as_ref(),
                    &cr,
                    log_segment.table_root.clone(),
                )
            })
            .transpose()?;

        let content_tree_leaf_state = metadata
            .as_ref()
            .map(|m| m.manifest_references())
            .transpose()?;
        let content_root_phase = metadata
            .as_ref()
            .map(|m| {
                m.root_action_batches(engine.as_ref(), crate::actions::get_commit_schema(), &[])
            })
            .transpose()?;

        Ok(Self {
            processor,
            commit_phase,
            checkpoint_manifest_phase,
            is_finished: false,
            checkpoint_parts,
            content_tree_leaf_state,
            content_root_phase,
        })
    }

    /// Complete sequential phase and extract processor + tasks for distribution.
    ///
    /// Must be called after the iterator is exhausted.
    ///
    /// # Parameters
    /// - `engine`: Engine for reading metadata root (only needed if ContentRoot was found)
    ///
    /// # Returns
    /// - `Done`: All processing done sequentially - no parallel phase needed
    /// - `Parallel`: Parallel phase needed. The resulting tasks should be distributed
    ///   across workers. Tasks can be either:
    ///   - `CheckpointFiles`: Checkpoint parts/sidecars to read and process
    ///   - `MetadataManifests`: Metadata tree manifests to read and process
    ///
    /// # Errors
    /// Returns an error if called before iterator exhaustion.
    #[allow(unused)]
    pub(crate) fn finish(self, engine: &dyn Engine) -> DeltaResult<AfterSequential<P>> {
        if !self.is_finished {
            return Err(Error::generic(
                "Must exhaust iterator before calling finish()",
            ));
        }

        // If ContentRoot was found, use the metadata leaf references extracted during initialization
        if let Some(leaf_references) = self.content_tree_leaf_state {
            // If there are manifest references, return them for parallel processing
            if !leaf_references.manifest_references.is_empty() {
                // The processor already has the shared metadata state from construction,
                // so we just return it along with the manifest references for parallel processing
                return Ok(AfterSequential::Parallel {
                    processor: self.processor,
                    tasks: PartitionTasks::MetadataManifests(leaf_references.manifest_references),
                });
            }
            // If no manifest references, fall through to check for other parallel work
        }

        let parallel_files = match self.checkpoint_manifest_phase {
            Some(manifest_reader) => manifest_reader.extract_sidecars()?,
            None => {
                let parts = self.checkpoint_parts;
                require!(
                    parts.len() != 1,
                    Error::generic(
                        "Invariant violation: If there is exactly one checkpoint part,
                        there must be a manifest reader"
                    )
                );
                // If this is a multi-part checkpoint, use the checkpoint parts for parallel phase
                parts
            }
        };

        if parallel_files.is_empty() {
            Ok(AfterSequential::Done(self.processor))
        } else {
            Ok(AfterSequential::Parallel {
                processor: self.processor,
                tasks: PartitionTasks::CheckpointFiles(parallel_files),
            })
        }
    }
}

impl<P: LogReplayProcessor> Iterator for SequentialPhase<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self
            .commit_phase
            .as_mut()
            .and_then(|commit_phase| commit_phase.next())
            .or_else(|| {
                self.commit_phase = None;
                self.checkpoint_manifest_phase.as_mut()?.next()
            })
            .or_else(|| {
                // Don't set checkpoint_manifest_phase to None - we need it in finish()
                // to extract sidecars
                self.content_root_phase.as_mut()?.next()
            });

        let Some(result) = next else {
            self.is_finished = true;
            return None;
        };

        // TODO: In process_actions_batch we whould defer returnining any action that has a data file
        // sourced from the tree (i.e. from the logs or checkpoint).  this is to accomoate DV
        // only updates that might not propogate stats into the log when returning the value.
        Some(result.and_then(|batch| self.processor.process_actions_batch(batch)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::state_info::StateInfo;
    use crate::utils::test_utils::{assert_result_error_with_message, load_test_table};
    use std::sync::Arc;
    use url::Url;

    /// Core helper function to verify sequential processing with expected adds and sidecars.
    fn verify_sequential_processing(
        table_name: &str,
        expected_adds: &[&str],
        expected_sidecars: &[&str],
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table(table_name)?;
        let log_segment = snapshot.log_segment();

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut sequential = SequentialPhase::try_new(processor, log_segment, engine.clone())?;

        // Process all batches and collect Add file paths
        let mut file_paths = Vec::new();
        for result in sequential.by_ref() {
            let metadata = result?;
            file_paths =
                metadata.visit_scan_files(file_paths, |ps: &mut Vec<String>, file_stat| {
                    ps.push(file_stat.path);
                })?;
        }

        // Assert collected adds match expected
        file_paths.sort();
        assert_eq!(
            file_paths, expected_adds,
            "Sequential phase should collect expected Add file paths"
        );

        // Call finish() and verify result based on expected sidecars
        let result = sequential.finish(engine.as_ref())?;
        match (expected_sidecars, result) {
            (sidecars, AfterSequential::Done(_)) => {
                assert!(
                    sidecars.is_empty(),
                    "Expected Done but got sidecars {:?}",
                    sidecars
                );
            }
            (expected_sidecars, AfterSequential::Parallel { tasks, .. }) => {
                let files = match tasks {
                    PartitionTasks::CheckpointFiles(files) => files,
                    PartitionTasks::MetadataManifests(_) => {
                        panic!("Expected CheckpointFiles but got MetadataManifests")
                    }
                };

                assert_eq!(
                    files.len(),
                    expected_sidecars.len(),
                    "Should collect exactly {} sidecar files",
                    expected_sidecars.len()
                );

                // Extract and verify sidecar file paths
                let mut collected_paths = files
                    .iter()
                    .map(|fm| {
                        fm.location
                            .path_segments()
                            .and_then(|mut segments| segments.next_back())
                            .unwrap_or("")
                            .to_string()
                    })
                    .collect_vec();

                collected_paths.sort();
                assert_eq!(collected_paths, expected_sidecars);
            }
        }

        Ok(())
    }

    #[test]
    fn test_sequential_v2_with_commits_only() -> DeltaResult<()> {
        verify_sequential_processing(
            "table-without-dv-small",
            &["part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"],
            &[], // No sidecars
        )
    }

    #[test]
    fn test_sequential_v2_with_sidecars() -> DeltaResult<()> {
        verify_sequential_processing(
            "v2-checkpoints-json-with-sidecars",
            &[], // No adds in sequential phase (all in checkpoint sidecars)
            &[
                "00000000000000000006.checkpoint.0000000001.0000000002.19af1366-a425-47f4-8fa6-8d6865625573.parquet",
                "00000000000000000006.checkpoint.0000000002.0000000002.5008b69f-aa8a-4a66-9299-0733a56a7e63.parquet",
            ],
        )
    }

    #[test]
    fn test_sequential_finish_before_exhaustion_error() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let log_segment = snapshot.log_segment();

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut sequential = SequentialPhase::try_new(processor, log_segment, engine.clone())?;

        // Call next() once but don't exhaust the iterator
        if let Some(result) = sequential.next() {
            result?;
        }

        // Try to call finish() before exhausting the iterator
        let result = sequential.finish(engine.as_ref());
        assert_result_error_with_message(result, "Must exhaust iterator before calling finish()");

        Ok(())
    }

    #[test]
    fn test_sequential_checkpoint_without_sidecars() -> DeltaResult<()> {
        verify_sequential_processing(
            "v2-checkpoints-json-without-sidecars",
            &[
                // Adds from checkpoint manifest processed in sequential phase
                "test%25file%25prefix-part-00000-0e32f92c-e232-4daa-b734-369d1a800502-c000.snappy.parquet",
                "test%25file%25prefix-part-00000-91daf7c5-9ba0-4f76-aefd-0c3b21d33c6c-c000.snappy.parquet",
                "test%25file%25prefix-part-00001-a5c41be1-ded0-4b18-a638-a927d233876e-c000.snappy.parquet",
            ],
            &[], // No sidecars
        )
    }

    #[test]
    fn test_sequential_parquet_checkpoint_with_sidecars() -> DeltaResult<()> {
        verify_sequential_processing(
            "v2-checkpoints-parquet-with-sidecars",
            &[], // No adds in sequential phase
            &[
                // Expected sidecars
                "00000000000000000006.checkpoint.0000000001.0000000002.76931b15-ead3-480d-b86c-afe55a577fc3.parquet",
                "00000000000000000006.checkpoint.0000000002.0000000002.4367b29c-0e87-447f-8e81-9814cc01ad1f.parquet",
            ],
        )
    }

    #[test]
    fn test_sequential_checkpoint_no_commits() -> DeltaResult<()> {
        verify_sequential_processing(
            "with_checkpoint_no_last_checkpoint",
            &["part-00000-70b1dcdf-0236-4f63-a072-124cdbafd8a0-c000.snappy.parquet"], // Add from commit 3
            &[],                                                                      // No sidecars
        )
    }

    // ========== Priority 1: Core ContentRoot Scenarios ==========
    // Helper functions for ContentRoot testing

    /// Helper to create a simple Add action for testing
    fn create_test_add(path: &str, size: i64, mod_time: i64) -> crate::actions::Add {
        crate::actions::Add {
            path: path.to_string(),
            partition_values: Default::default(),
            size,
            modification_time: mod_time,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            data_manifest_path: None,
            data_manifest_position: None,
            delete_manifest_path: None,
            delete_manifest_position: None,
        }
    }

    /// Helper to setup a test table with ContentRoot and optionally checkpoint parts
    fn setup_contentroot_test_table(
        add_files: Vec<&str>,
        checkpoint_parts: Option<Vec<crate::path::ParsedLogPath>>,
    ) -> DeltaResult<(
        Arc<dyn Engine>,
        crate::log_segment::LogSegment,
        Vec<String>, // Expected file paths from ContentRoot
    )> {
        use crate::engine::default::DefaultEngine;
        use crate::metadata::builder::MetadataBuilder;
        use crate::metadata::writer::MetadataWriter;
        use crate::path::{LogPathFileType, ParsedLogPath};
        use futures::executor::block_on;
        use object_store::{memory::InMemory, path::Path, ObjectStore};

        let store = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let log_root = table_root.join("_delta_log/").unwrap();

        // Create initial commit with protocol and metadata
        let commit0_content = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}
{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#;
        block_on(async {
            store
                .put(
                    &Path::from("_delta_log/00000000000000000000.json"),
                    commit0_content.into(),
                )
                .await
        })
        .unwrap();

        let engine = Arc::new(DefaultEngine::new(store.clone()));

        // Create metadata for content_root with Add actions
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1);
        for file_path in &add_files {
            builder
                .add(create_test_add(file_path, 2048, 1677811178336), 1, None)
                .unwrap();
        }

        let metadata = builder.build(engine.as_ref()).unwrap();
        let writer = MetadataWriter::try_new(metadata).unwrap();
        let content_root_url = writer.write(engine.as_ref()).unwrap();

        // Create commit with contentRoot action
        let commit1_content = format!(
            r#"{{"contentRoot":{{"path":"{}","sizeInBytes":1024}}}}"#,
            content_root_url
        );
        block_on(async {
            store
                .put(
                    &Path::from("_delta_log/00000000000000000001.json"),
                    commit1_content.into(),
                )
                .await
        })
        .unwrap();

        // Create commit ParsedLogPath objects
        let mut commit_files = vec![];
        for version in 0..=1 {
            let location = log_root.join(&format!("{:020}.json", version)).unwrap();
            commit_files.push(ParsedLogPath {
                location: FileMeta {
                    location,
                    last_modified: 0,
                    size: 100,
                },
                filename: format!("{:020}.json", version),
                extension: "json".to_string(),
                version,
                file_type: LogPathFileType::Commit,
            });
        }

        let checkpoint_version = checkpoint_parts.as_ref().map(|_| 1);
        let log_segment = crate::log_segment::LogSegment {
            end_version: 1,
            checkpoint_version,
            log_root: log_root.clone(),
            table_root: table_root.clone(),
            ascending_commit_files: commit_files,
            ascending_compaction_files: vec![],
            checkpoint_parts: checkpoint_parts.unwrap_or_default(),
            latest_crc_file: None,
            latest_commit_file: None,
            checkpoint_schema: None,
        };

        let expected_paths: Vec<String> = add_files.iter().map(|s| s.to_string()).collect();
        Ok((engine, log_segment, expected_paths))
    }

    /// Test 1.3: ContentRoot Only (No Parallel Work)
    /// Purpose: Verify ContentRoot with no manifest refs and no checkpoint files completes sequentially
    #[test]
    fn test_contentroot_only_no_parallel_work() -> DeltaResult<()> {
        // Setup: Create table with 2 files in ContentRoot, no checkpoint
        let (engine, log_segment, expected_files) = setup_contentroot_test_table(
            vec!["part-content-root-1.parquet", "part-content-root-2.parquet"],
            None, // No checkpoint parts
        )?;

        // Create snapshot and processor
        let snapshot = Arc::new(crate::snapshot::Snapshot::try_new_from_log_segment(
            Url::parse("memory:///").unwrap(),
            log_segment.clone(),
            engine.as_ref(),
            None,
        )?);

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut sequential = SequentialPhase::try_new(processor, &log_segment, engine.clone())?;

        // Process all batches and collect Add file paths
        let mut file_paths = Vec::new();
        for result in sequential.by_ref() {
            let metadata = result?;
            file_paths =
                metadata.visit_scan_files(file_paths, |ps: &mut Vec<String>, file_stat| {
                    ps.push(file_stat.path);
                })?;
        }

        // Verify files from ContentRoot were processed
        file_paths.sort();
        assert_eq!(
            file_paths, expected_files,
            "Should have collected files from ContentRoot"
        );

        // Call finish() - should return Done since no parallel work
        let result = sequential.finish(engine.as_ref())?;
        match result {
            AfterSequential::Done(_processor) => {
                // Expected - no parallel work needed
            }
            AfterSequential::Parallel { .. } => {
                panic!("Expected Done but got Parallel");
            }
        }

        Ok(())
    }

    /// Test 1.2: ContentRoot without Manifest References but with Checkpoint Files
    /// Purpose: Verify ContentRoot without child manifests doesn't block other parallel work
    #[test]
    fn test_contentroot_no_manifests_with_checkpoint_files() -> DeltaResult<()> {
        use crate::path::{LogPathFileType, ParsedLogPath};

        // Create multi-part checkpoint parts
        let table_root = Url::parse("memory:///").unwrap();
        let log_root = table_root.join("_delta_log/").unwrap();
        let checkpoint_parts = vec![
            ParsedLogPath {
                location: FileMeta {
                    location: log_root
                        .join("00000000000000000001.checkpoint.0000000001.0000000002.parquet")
                        .unwrap(),
                    last_modified: 0,
                    size: 1024,
                },
                filename: "00000000000000000001.checkpoint.0000000001.0000000002.parquet"
                    .to_string(),
                extension: "parquet".to_string(),
                version: 1,
                file_type: LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts: 2,
                },
            },
            ParsedLogPath {
                location: FileMeta {
                    location: log_root
                        .join("00000000000000000001.checkpoint.0000000002.0000000002.parquet")
                        .unwrap(),
                    last_modified: 0,
                    size: 1024,
                },
                filename: "00000000000000000001.checkpoint.0000000002.0000000002.parquet"
                    .to_string(),
                extension: "parquet".to_string(),
                version: 1,
                file_type: LogPathFileType::MultiPartCheckpoint {
                    part_num: 2,
                    num_parts: 2,
                },
            },
        ];

        // Setup: Create table with 1 file in ContentRoot + multi-part checkpoint
        let (engine, log_segment, expected_files) = setup_contentroot_test_table(
            vec!["part-content-root.parquet"],
            Some(checkpoint_parts),
        )?;

        // Create snapshot and processor
        let snapshot = Arc::new(crate::snapshot::Snapshot::try_new_from_log_segment(
            table_root.clone(),
            log_segment.clone(),
            engine.as_ref(),
            None,
        )?);

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut sequential = SequentialPhase::try_new(processor, &log_segment, engine.clone())?;

        // Process all batches and collect Add file paths
        let mut file_paths = Vec::new();
        for result in sequential.by_ref() {
            let metadata = result?;
            file_paths =
                metadata.visit_scan_files(file_paths, |ps: &mut Vec<String>, file_stat| {
                    ps.push(file_stat.path);
                })?;
        }

        // Verify file from ContentRoot was processed
        assert_eq!(
            file_paths, expected_files,
            "Should have 1 file from ContentRoot"
        );

        // Call finish() - should return CheckpointFiles for parallel processing
        // (not MetadataManifests, since ContentRoot has no child manifests)
        let result = sequential.finish(engine.as_ref())?;
        match result {
            AfterSequential::Parallel { tasks, .. } => match tasks {
                PartitionTasks::CheckpointFiles(files) => {
                    assert_eq!(files.len(), 2, "Should have 2 checkpoint files");
                }
                PartitionTasks::MetadataManifests(_) => {
                    panic!("Expected CheckpointFiles but got MetadataManifests");
                }
            },
            AfterSequential::Done(_) => {
                panic!("Expected Parallel work but got Done");
            }
        }

        Ok(())
    }
}
