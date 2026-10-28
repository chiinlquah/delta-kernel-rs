//! Borrowed inspection of the files retained by a snapshot's log segment.

use delta_kernel::log_segment::LogSegment;
use delta_kernel::path::ParsedLogPath;

use crate::handle::Handle;
use crate::log_path::{FfiLogPath, LogPathArray};
use crate::{
    kernel_string_slice, optional_pointer, FfiSlice, KernelStringSlice, NullableCvoid,
    OptionalValue, SharedSnapshot,
};

/// The retained files and version bounds of a snapshot's log segment.
///
/// All pointers, arrays, and strings are borrowed until the callback returns. No metadata,
/// protocol, CRC contents, or checkpoint contents are exported.
#[repr(C)]
pub struct FfiLogSegment {
    /// Encoded URL of the transaction log directory.
    pub log_root: KernelStringSlice,
    /// Snapshot version described by this segment.
    pub end_version: u64,
    /// Selected checkpoint version, or None when replay starts from version zero.
    pub checkpoint_version: OptionalValue<u64>,
    /// Replay commits, including staged commits, in ascending version order.
    pub ascending_commit_files: LogPathArray,
    /// Retained log compaction files in ascending version order.
    pub ascending_compaction_files: LogPathArray,
    /// Parts of the selected complete checkpoint.
    pub checkpoint_parts: LogPathArray,
    /// Terminal commit, retained separately from replay files. Null means absent.
    pub latest_commit_file: *const FfiLogPath,
    /// Latest retained checksum file. Null means absent.
    pub latest_crc_file: *const FfiLogPath,
    /// Highest observed published version, independent of the retained staged paths.
    pub max_published_version: OptionalValue<u64>,
}

/// Receives one complete log segment whose nested storage expires when the callback returns.
pub type LogSegmentVisitor = extern "C" fn(NullableCvoid, *const FfiLogSegment);

/// Inspects a snapshot's retained log segment without I/O or snapshot-hint construction.
///
/// Invokes `visitor` once, including for checkpoint-only snapshots with no retained commit.
/// The snapshot is borrowed; no handle or result needs to be released by the callback.
///
/// # Safety
///
/// The snapshot handle must remain valid throughout the call. The callback must not consume or
/// free it, retain borrowed pointers, or unwind through the native frame. `context` must satisfy
/// the callback's contract.
#[no_mangle]
pub unsafe extern "C" fn snapshot_log_segment(
    snapshot: Handle<SharedSnapshot>,
    context: NullableCvoid,
    visitor: LogSegmentVisitor,
) {
    let snapshot = unsafe { snapshot.as_ref() };
    visit(snapshot.log_segment(), context, visitor);
}

fn path(value: &ParsedLogPath) -> FfiLogPath {
    let meta = &value.location;
    let location = meta.location.as_str();
    FfiLogPath::new(
        kernel_string_slice!(location),
        meta.last_modified,
        meta.size,
    )
}

fn visit(segment: &LogSegment, context: NullableCvoid, visitor: LogSegmentVisitor) {
    let files = &segment.listed;
    let commits: Vec<_> = files.ascending_commit_files.iter().map(path).collect();
    let compactions: Vec<_> = files.ascending_compaction_files.iter().map(path).collect();
    let checkpoints: Vec<_> = files.checkpoint_parts.iter().map(path).collect();
    let latest_commit = files.latest_commit_file.as_ref().map(path);
    let latest_crc = files.latest_crc_file.as_ref().map(path);
    let root = segment.log_root.as_str();
    let value = FfiLogSegment {
        log_root: kernel_string_slice!(root),
        end_version: segment.end_version,
        checkpoint_version: segment.checkpoint_version.into(),
        ascending_commit_files: unsafe { FfiSlice::new_unsafe(&commits) },
        ascending_compaction_files: unsafe { FfiSlice::new_unsafe(&compactions) },
        checkpoint_parts: unsafe { FfiSlice::new_unsafe(&checkpoints) },
        latest_commit_file: optional_pointer(latest_commit.as_ref()),
        latest_crc_file: optional_pointer(latest_crc.as_ref()),
        max_published_version: files.max_published_version.into(),
    };
    visitor(context, &value);
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;

    use delta_kernel::log_segment_files::LogSegmentFiles;
    use delta_kernel::FileMeta;
    use test_utils::create_log_path;

    use super::*;
    use crate::TryFromStringSlice;

    #[derive(Clone, Debug, PartialEq)]
    struct Captured {
        root: String,
        end: u64,
        checkpoint: Option<u64>,
        commits: Vec<FileMeta>,
        compactions: Vec<FileMeta>,
        checkpoints: Vec<FileMeta>,
        latest_commit: Option<FileMeta>,
        crc: Option<FileMeta>,
        published: Option<u64>,
    }

    fn copy_path(value: &FfiLogPath) -> FileMeta {
        FileMeta {
            location: unsafe { String::try_from_slice(value.location()) }
                .unwrap()
                .parse()
                .unwrap(),
            last_modified: value.last_modified(),
            size: value.size(),
        }
    }

    extern "C" fn capture(context: NullableCvoid, value: *const FfiLogSegment) {
        let out = unsafe { context.unwrap().cast::<Vec<Captured>>().as_mut() };
        let value = unsafe { &*value };
        let copy = |paths: &LogPathArray| {
            unsafe { paths.try_as_slice() }
                .unwrap()
                .iter()
                .map(copy_path)
                .collect()
        };
        out.push(Captured {
            root: unsafe { String::try_from_slice(&value.log_root) }.unwrap(),
            end: value.end_version,
            checkpoint: Option::<&u64>::from(&value.checkpoint_version).copied(),
            commits: copy(&value.ascending_commit_files),
            compactions: copy(&value.ascending_compaction_files),
            checkpoints: copy(&value.checkpoint_parts),
            latest_commit: unsafe { value.latest_commit_file.as_ref() }.map(copy_path),
            crc: unsafe { value.latest_crc_file.as_ref() }.map(copy_path),
            published: Option::<&u64>::from(&value.max_published_version).copied(),
        });
    }

    #[rstest::rstest]
    #[case::checkpoint_only(false, false)]
    #[case::checkpoint_with_terminal(false, true)]
    #[case::staged_tail(true, true)]
    fn borrowed_log_segment_export_preserves_files_and_absence(
        #[case] with_tail: bool,
        #[case] with_terminal: bool,
    ) {
        let root = "memory:///literal%252a/_delta_log/";
        let make_path = |name: &str| -> ParsedLogPath { create_log_path(root, name.into()).into() };
        let checkpoint = make_path("00000000000000000000.checkpoint.parquet");
        let mut terminal = make_path(if with_tail {
            "_staged_commits/00000000000000000001.00000000-0000-0000-0000-000000000001.json"
        } else {
            "00000000000000000000.json"
        });
        terminal.location.size = u64::MAX;
        terminal.location.last_modified = -17;
        let crc = make_path(if with_tail {
            "00000000000000000001.crc"
        } else {
            "00000000000000000000.crc"
        });
        let end = u64::from(with_tail);
        let segment = LogSegment::try_new(
            LogSegmentFiles {
                ascending_commit_files: if with_tail {
                    vec![terminal.clone()]
                } else {
                    vec![]
                },
                checkpoint_parts: vec![checkpoint.clone()],
                latest_commit_file: with_terminal.then_some(terminal.clone()),
                latest_crc_file: Some(crc.clone()),
                max_published_version: with_terminal.then_some(0),
                ..Default::default()
            },
            root.parse().unwrap(),
            Some(end),
            None,
        )
        .unwrap();
        let expected = Captured {
            root: root.into(),
            end,
            checkpoint: Some(0),
            commits: if with_tail {
                vec![terminal.location.clone()]
            } else {
                vec![]
            },
            compactions: vec![],
            checkpoints: vec![checkpoint.location],
            latest_commit: with_terminal.then_some(terminal.location),
            crc: Some(crc.location),
            published: with_terminal.then_some(0),
        };
        let mut captured: Vec<Captured> = Vec::new();
        for _ in 0..2 {
            visit(&segment, Some(NonNull::from(&mut captured).cast()), capture);
        }
        drop(segment);
        assert_eq!(captured, vec![expected.clone(), expected]);
    }
}
