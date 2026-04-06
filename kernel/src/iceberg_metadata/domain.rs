//! Iceberg metadata domain for tracking metadata.json state across Delta commits.
//!
//! The `IcebergMetadataDomain` is stored as a [`DomainMetadata`] action in the Delta log
//! under domain `"com.databricks.iceberg.metadata"`. It tracks the location of the
//! latest Iceberg metadata.json and the snapshot IDs produced.

use serde::{Deserialize, Serialize};

use crate::actions::DomainMetadata;
use crate::{DeltaResult, Error};

/// Domain name for Iceberg metadata tracking in Delta log.
pub(crate) const ICEBERG_METADATA_DOMAIN: &str = "com.databricks.iceberg.metadata";

/// Tracks the Iceberg metadata.json state across Delta commits.
///
/// This is serialized as the `configuration` field of a [`DomainMetadata`] action
/// with domain [`ICEBERG_METADATA_DOMAIN`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IcebergMetadataDomain {
    /// The Delta commit version this domain metadata corresponds to.
    pub delta_commit_version: i64,
    /// The Iceberg snapshot ID for the current Delta commit.
    pub current_snapshot_id: Option<i64>,
    /// Snapshot IDs for new Iceberg snapshots created for this commit.
    pub new_snapshot_ids: Vec<i64>,
    /// Location of the Iceberg metadata.json file.
    pub metadata_location: Option<String>,
}

impl IcebergMetadataDomain {
    /// Creates a new `IcebergMetadataDomain` for a commit.
    pub(crate) fn new(
        delta_commit_version: i64,
        snapshot_id: i64,
        metadata_location: String,
    ) -> Self {
        Self {
            delta_commit_version,
            current_snapshot_id: Some(snapshot_id),
            new_snapshot_ids: vec![snapshot_id],
            metadata_location: Some(metadata_location),
        }
    }

    /// Converts this domain metadata into a [`DomainMetadata`] action for the Delta log.
    pub(crate) fn to_domain_metadata(&self) -> DeltaResult<DomainMetadata> {
        let configuration = serde_json::to_string(self).map_err(|e| {
            Error::generic(format!(
                "Failed to serialize IcebergMetadataDomain: {}",
                e
            ))
        })?;
        Ok(DomainMetadata::new(
            ICEBERG_METADATA_DOMAIN.to_string(),
            configuration,
        ))
    }

    /// Parses an `IcebergMetadataDomain` from a [`DomainMetadata`] action.
    pub(crate) fn from_domain_metadata(dm: &DomainMetadata) -> DeltaResult<Self> {
        if dm.domain() != ICEBERG_METADATA_DOMAIN {
            return Err(Error::generic(format!(
                "Expected domain '{}', got '{}'",
                ICEBERG_METADATA_DOMAIN,
                dm.domain()
            )));
        }
        serde_json::from_str(dm.configuration()).map_err(|e| {
            Error::generic(format!(
                "Failed to parse IcebergMetadataDomain: {}",
                e
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_serialization() {
        let domain = IcebergMetadataDomain::new(
            42,
            123456789,
            "s3://bucket/table/__iceberg/metadata/abc.metadata.json".to_string(),
        );

        let dm = domain.to_domain_metadata().unwrap();
        assert_eq!(dm.domain(), ICEBERG_METADATA_DOMAIN);

        let parsed = IcebergMetadataDomain::from_domain_metadata(&dm).unwrap();
        assert_eq!(parsed.delta_commit_version, 42);
        assert_eq!(parsed.current_snapshot_id, Some(123456789));
        assert_eq!(parsed.new_snapshot_ids, vec![123456789]);
        assert_eq!(
            parsed.metadata_location.unwrap(),
            "s3://bucket/table/__iceberg/metadata/abc.metadata.json"
        );
    }

    #[test]
    fn json_format_matches_dbr() {
        let domain = IcebergMetadataDomain::new(
            10,
            9999,
            "s3://bucket/metadata.json".to_string(),
        );

        let json = serde_json::to_value(&domain).unwrap();
        assert_eq!(json["deltaCommitVersion"], 10);
        assert_eq!(json["currentSnapshotId"], 9999);
        assert_eq!(json["newSnapshotIds"], serde_json::json!([9999]));
        assert_eq!(json["metadataLocation"], "s3://bucket/metadata.json");
    }
}
