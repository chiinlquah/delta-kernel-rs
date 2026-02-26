//! Stats field ID calculation utilities for Adaptive ContentTreeNode Tree (AMT).
//!
//! This module provides functions to compute stats field IDs for parent struct fields,
//! which are used in the AMT format for storing per-column statistics.

use crate::content_tree::NULL_COUNT_FIELD_NAME;
use crate::expressions::{Expression, ExpressionRef, Scalar, StructData, Transform};
use crate::schema::visitor::{visit_struct, SchemaVisitor};
use crate::schema::{
    ArrayType, ColumnMetadataKey, ColumnName, DataType, MapType, MetadataValue, PrimitiveType,
    StructField, StructType,
};
use crate::{DeltaResult, Engine, EngineData};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Number of supported stats per column.
const NUM_SUPPORTED_STATS_PER_COLUMN: i32 = 200;

/// Number of reserved field IDs at the top of the i32 range.
const NUM_RESERVED_FIELD_IDS: i32 = 200;

/// Starting field ID of the stats space for the data field IDs (regular column stats).
const STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS: i32 = 10_000;

/// Starting field ID of the stats space for the metadata field IDs (reserved field stats).
const STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS: i32 = 2_147_000_000;

/// Field ID where reserved field IDs begin.
const RESERVED_FIELD_IDS_START: i32 = i32::MAX - NUM_RESERVED_FIELD_IDS;

/// Computes the base field ID for a column's stats struct, given a parent struct field ID.
///
/// Base stats field IDs are computed in different "spaces" depending on the input field ID:
/// - Regular field IDs (0 to `RESERVED_FIELD_IDS_START - 1`) use the data space starting at 10,000
/// - Reserved field IDs (`RESERVED_FIELD_IDS_START` to `i32::MAX`) use the metadata space
///   starting at 2,147,000,000
///
/// # Arguments
///
/// * `field_id` - The parent struct field ID to compute the base stats field ID for
///
/// # Returns
///
/// Returns `Some(base_stats_field_id)` if the computation succeeds, or `None` if the computed
/// field ID would overflow or overlap with another ID range.
///
/// # Examples
///
/// ```ignore
/// let stats_id = field_id_to_statistics_base(0);
/// assert_eq!(stats_id, Some(10_000));
///
/// let stats_id = field_id_to_statistics_base(1);
/// assert_eq!(stats_id, Some(10_200));
/// ```
pub(crate) fn field_id_to_statistics_base(field_id: i32) -> Option<i32> {
    if field_id < 0 {
        // Short circuit on negative field-IDs
        return None;
    }

    let (id_space_start, id) = if field_id >= RESERVED_FIELD_IDS_START {
        // This is a reserved field ID, which uses a different calculation
        let id = NUM_RESERVED_FIELD_IDS - (i32::MAX - field_id);
        (STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS, id)
    } else {
        (STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS, field_id)
    };

    // Calculate final_id, checking for overflow
    let stats_offset = NUM_SUPPORTED_STATS_PER_COLUMN.checked_mul(id)?;
    let final_id = id_space_start.checked_add(stats_offset)?;

    // Check for overlap with other ID ranges:
    // Data space IDs should not overlap into metadata space
    if field_id < RESERVED_FIELD_IDS_START
        && final_id >= STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS
    {
        return None;
    }

    Some(final_id)
}

/// Computes the original field ID from a stats field ID.
///
/// This is the inverse of [`field_id_to_statistics_base`]. Given a stats field ID,
/// it returns the original parent struct field ID that would produce that stats field ID.
///
/// # Arguments
///
/// * `stats_field_id` - The stats field ID to convert back to a field ID
///
/// # Returns
///
/// Returns `Some(field_id)` if the stats field ID is valid, or `None` if:
/// - The stats field ID is negative
/// - The stats field ID is not a multiple of `NUM_STATS_PER_COLUMN` (200)
/// - The resulting field ID would be negative
///
/// # Examples
///
/// ```ignore
/// let field_id = statistics_base_to_field_id(10_000);
/// assert_eq!(field_id, Some(0));
///
/// let field_id = statistics_base_to_field_id(10_200);
/// assert_eq!(field_id, Some(1));
/// ```
#[cfg(test)]
pub(crate) fn statistics_base_to_field_id(stats_field_id: i32) -> Option<i32> {
    // Invalid stats field ID: negative or not a multiple of NUM_STATS_PER_COLUMN
    if stats_field_id < 0 || stats_field_id % NUM_SUPPORTED_STATS_PER_COLUMN != 0 {
        return None;
    }

    let final_id = if stats_field_id < STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS {
        // Data space: reverse the calculation
        // stats_field_id = DATA_SPACE_FIELD_ID_START + NUM_STATS_PER_COLUMN * field_id
        // => field_id = (stats_field_id - DATA_SPACE_FIELD_ID_START) / NUM_STATS_PER_COLUMN
        (stats_field_id - STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS)
            / NUM_SUPPORTED_STATS_PER_COLUMN
    } else {
        // ContentTreeNode space (reserved field IDs): reverse the calculation
        // stats_field_id = METADATA_SPACE_FIELD_ID_START + NUM_STATS_PER_COLUMN * id
        // where id = RESERVED_FIELD_IDS - (i32::MAX - field_id)
        // => id = (stats_field_id - METADATA_SPACE_FIELD_ID_START) / NUM_STATS_PER_COLUMN
        // => field_id = i32::MAX - (RESERVED_FIELD_IDS - id) = RESERVED_FIELD_IDS_START + id
        let id = (stats_field_id - STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS)
            / NUM_SUPPORTED_STATS_PER_COLUMN;
        RESERVED_FIELD_IDS_START + id
    };

    // Return None if final_id is negative (invalid stats field ID for data space)
    if final_id < 0 {
        None
    } else {
        Some(final_id)
    }
}

/// Field ID offsets for stats fields within a column's stats struct.
const STATS_OFFSET_VALUE_COUNT: i32 = 1;
const STATS_OFFSET_NULL_VALUE_COUNT: i32 = 2;
const STATS_OFFSET_NAN_VALUE_COUNT: i32 = 3;
const STATS_OFFSET_AVG_VALUE_SIZE: i32 = 4;
const STATS_OFFSET_MAX_VALUE_SIZE: i32 = 5;
const STATS_OFFSET_LOWER_BOUND: i32 = 6;
const STATS_OFFSET_UPPER_BOUND: i32 = 7;
const STATS_OFFSET_EXACT_BOUNDS: i32 = 8;

/// Creates a [`StructField`] with the given name, data type, nullability, and field ID.
/// Also includes column mapping annotations (`delta.columnMapping.id` and
/// `delta.columnMapping.physicalName`) which are required when the metadata tree feature
/// is enabled (as it requires column mapping to be enabled).
fn field_with_id(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> StructField {
    StructField::new(name, data_type, nullable).with_metadata([
        (
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::Number(field_id as i64),
        ),
        (
            ColumnMetadataKey::ColumnMappingId.as_ref(),
            MetadataValue::Number(field_id as i64),
        ),
        (
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            MetadataValue::String(format!("col-{}", name)),
        ),
    ])
}

/// Extracts the parquet field ID from a StructField's metadata.
fn get_field_id(field: &StructField) -> Option<i32> {
    match field
        .metadata()
        .get(ColumnMetadataKey::ParquetFieldId.as_ref())
    {
        Some(MetadataValue::Number(id)) => Some(*id as i32),
        _ => None,
    }
}

/// A visitor that builds stats schemas by traversing the data schema.
struct StatsSchemaVisitor;

impl SchemaVisitor for StatsSchemaVisitor {
    type T = Vec<StructField>;

    fn primitive(&mut self, _ptype: &PrimitiveType) -> DeltaResult<Self::T> {
        Ok(Vec::new())
    }

    fn field(&mut self, field: &StructField, type_result: Self::T) -> DeltaResult<Self::T> {
        // Get the field ID from metadata
        let field_id = get_field_id(field).ok_or_else(|| {
            crate::Error::generic(format!(
                "Field '{}' is missing field ID! ContentTreeNode: {:#?}",
                field.name(),
                field.metadata()
            ))
        })?;

        // Compute the base stats field ID
        let base_stats_id = field_id_to_statistics_base(field_id).ok_or_else(|| {
            crate::Error::generic(format!(
                "Failed to compute stats field ID for field '{}' with id {}",
                field.name(),
                field_id
            ))
        })?;

        // Build the stats struct based on the field's data type.
        let stats_struct = match field.data_type() {
            DataType::Primitive(_) => {
                build_primitive_stats_struct(base_stats_id, field.data_type.clone(), field.nullable)
            }
            _ => StructType::new_unchecked(type_result),
        };

        // Build metadata with the base_stats_id as the field ID instead of the original field ID.
        // The stats group struct should use the base stats field ID (e.g., 10200 for field_id=1),
        // not the original column field ID.
        let metadata: Vec<(&str, MetadataValue)> = field
            .metadata
            .iter()
            .map(|(k, v)| {
                let value = if k == ColumnMetadataKey::ParquetFieldId.as_ref()
                    || k == ColumnMetadataKey::ColumnMappingId.as_ref()
                {
                    MetadataValue::Number(base_stats_id as i64)
                } else {
                    v.clone()
                };
                (k.as_str(), value)
            })
            .collect();

        Ok(vec![StructField::new(
            field.name(),
            DataType::Struct(Box::new(stats_struct)),
            true,
        )
        .with_metadata(metadata)])
    }

    fn r#struct(&mut self, _struct: &StructType, results: Vec<Self::T>) -> DeltaResult<Self::T> {
        Ok(results.into_iter().flatten().collect())
    }

    fn list(&mut self, _list: &ArrayType, element_result: Self::T) -> DeltaResult<Self::T> {
        // TODO: Missing field-id on element
        Ok(element_result)
    }

    fn map(
        &mut self,
        _map: &MapType,
        key_result: Self::T,
        value_result: Self::T,
    ) -> DeltaResult<Self::T> {
        // TODO: Missing field-id on key
        // TODO: Missing field-id on value
        Ok(key_result.into_iter().chain(value_result).collect())
    }

    fn variant(&mut self, _struct: &StructType) -> DeltaResult<Self::T> {
        // TODO: Variant stats
        Ok(Vec::new())
    }
}

/// Builds the stats struct for a primitive field.
///
/// The stats struct contains the following fields (with field IDs as offsets from the base):
/// - offset 1: `value_count` (long)
/// - offset 2: `null_value_count` (long) - only if the field is nullable
/// - offset 3: `nan_value_count` (long) - only for float/double types
/// - offset 4: `avg_value_size` (int) - only for variable-length types (e.g. string/binary)
/// - offset 5: `max_value_size` (int) - only for variable-length types (e.g. string/binary)
/// - offset 6: `lower_bound` (same type as the field)
/// - offset 7: `upper_bound` (same type as the field)
/// - offset 8: `exact_bounds` (boolean)
fn build_primitive_stats_struct(
    base_field_id: i32,
    data_type: DataType,
    nullable: bool,
) -> StructType {
    // Base fields: value_count, lower_bound, upper_bound, exact_bounds.
    // Optional fields:
    // - null_value_count (if nullable)
    // - nan_value_count (if float/double)
    // - avg_value_size/max_value_size (if variable-length: string/binary)
    let (has_nan_count, has_size_stats) = match &data_type {
        DataType::Primitive(ptype) => (
            matches!(ptype, &PrimitiveType::Float | &PrimitiveType::Double),
            matches!(ptype, &PrimitiveType::String | &PrimitiveType::Binary),
        ),
        _ => (false, false),
    };

    let capacity =
        4 + usize::from(nullable) + usize::from(has_nan_count) + if has_size_stats { 2 } else { 0 };
    let mut fields = Vec::with_capacity(capacity);

    // value_count: always present
    fields.push(field_with_id(
        "value_count",
        DataType::LONG,
        true,
        base_field_id + STATS_OFFSET_VALUE_COUNT,
    ));

    // null_value_count: only if the field is nullable
    if nullable {
        fields.push(field_with_id(
            crate::content_tree::NULL_COUNT_FIELD_NAME,
            DataType::LONG,
            true,
            base_field_id + STATS_OFFSET_NULL_VALUE_COUNT,
        ));
    }

    // nan_value_count: only for float/double types
    if has_nan_count {
        fields.push(field_with_id(
            "nan_value_count",
            DataType::LONG,
            true,
            base_field_id + STATS_OFFSET_NAN_VALUE_COUNT,
        ));
    }

    // avg_value_size/max_value_size: only for variable-length types (e.g. string/binary)
    if has_size_stats {
        fields.push(field_with_id(
            "avg_value_size",
            DataType::INTEGER,
            true,
            base_field_id + STATS_OFFSET_AVG_VALUE_SIZE,
        ));

        fields.push(field_with_id(
            "max_value_size",
            DataType::INTEGER,
            true,
            base_field_id + STATS_OFFSET_MAX_VALUE_SIZE,
        ));
    }

    // lower_bound: same type as the field
    fields.push(field_with_id(
        "lower_bound",
        data_type.clone(),
        true,
        base_field_id + STATS_OFFSET_LOWER_BOUND,
    ));

    // upper_bound: same type as the field
    fields.push(field_with_id(
        "upper_bound",
        data_type.clone(),
        true,
        base_field_id + STATS_OFFSET_UPPER_BOUND,
    ));

    // exact_bounds: always present
    fields.push(field_with_id(
        "exact_bounds",
        DataType::BOOLEAN,
        true,
        base_field_id + STATS_OFFSET_EXACT_BOUNDS,
    ));

    StructType::new_unchecked(fields)
}

/// Generates a stats schema for the given struct type.
///
/// This function traverses the schema using a post-order visitor and generates
/// a corresponding stats schema that mirrors the structure of the input schema.
///
/// ## Stats Schema Structure
///
/// Each primitive field's stats struct contains:
/// - `value_count` (long): count of values
/// - `null_value_count` (long): count of null values (only if field is nullable)
/// - `nan_value_count` (long): count of NaN values (only for float/double types)
/// - `avg_value_size` (int): average size of values (only for variable-length types)
/// - `max_value_size` (int): maximum size of values (only for variable-length types)
/// - `lower_bound` (same type as field): minimum value
/// - `upper_bound` (same type as field): maximum value
/// - `exact_bounds` (boolean): whether bounds are exact
///
/// ## Field IDs
///
/// Field IDs in the stats schema are computed using [`field_id_to_statistics_base`]
/// based on the original field's `PARQUET:field_id` metadata.
///
/// # Arguments
///
/// * `table_struct` - The struct type to generate stats schema for
///
/// # Returns
///
/// Returns `Ok(StructType)` containing the stats schema, or an error if:
/// - Any field is missing the `PARQUET:field_id` metadata
/// - Field ID computation fails (e.g., overflow)
pub(crate) fn stats_schema(table_struct: &StructType) -> DeltaResult<StructType> {
    let mut visitor = StatsSchemaVisitor;
    let fields = visit_struct(table_struct, &mut visitor)?;
    Ok(StructType::new_unchecked(fields))
}

/// Delta Protocol JSON stats format.
///
/// This struct represents the statistics stored in the `stats` field of Add actions
/// in Delta Lake format. The format is:
/// ```json
/// {
///     "numRecords": 100,
///     "minValues": {"col1": 0, "col2": "a"},
///     "maxValues": {"col1": 10, "col2": "z"},
///     "nullCount": {"col1": 0, "col2": 5}
/// }
/// ```
///
/// The optional `tightBounds` field indicates whether the statistics are exact:
/// - `true` (or absent): bounds are tight/exact, accurately representing the data
/// - `false`: bounds may be wider than actual data (e.g., due to deletion vectors)
#[derive(Debug, Clone, Default)]
struct DeltaJsonStats {
    num_records: Option<i64>,
    min_values: HashMap<String, JsonValue>,
    max_values: HashMap<String, JsonValue>,
    null_count: HashMap<String, i64>,
    /// Whether the min/max bounds are tight (exact). Defaults to true when not present.
    /// When false, the bounds may be wider than the actual data due to deletion vectors
    /// or other operations that logically remove rows without updating statistics.
    tight_bounds: bool,
}

impl DeltaJsonStats {
    /// Parse a JSON stats string from Delta Protocol format.
    fn parse(json_str: &str) -> Option<Self> {
        // TODO: We should delegate this to the engine.. at some point...
        let parsed: JsonValue = serde_json::from_str(json_str).ok()?;
        let obj = parsed.as_object()?;

        let num_records = obj
            .get("numRecords")
            .and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|u| u as i64)));

        let min_values = obj
            .get("minValues")
            .and_then(|v| v.as_object())
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let max_values = obj
            .get("maxValues")
            .and_then(|v| v.as_object())
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let null_count = obj
            .get("nullCount")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| {
                        v.as_i64()
                            .or_else(|| v.as_u64().map(|u| u as i64))
                            .map(|count| (k.clone(), count))
                    })
                    .collect()
            })
            .unwrap_or_default();

        // tightBounds defaults to true when not present (for backwards compatibility).
        // When false, the bounds may be wider than the actual data (e.g., due to deletion vectors).
        let tight_bounds = obj
            .get("tightBounds")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        Some(Self {
            num_records,
            min_values,
            max_values,
            null_count,
            tight_bounds,
        })
    }
}

/// Converts a JSON value to a Scalar based on the expected data type.
fn json_value_to_scalar(value: &JsonValue, data_type: &DataType) -> Option<Scalar> {
    match data_type {
        DataType::Primitive(ptype) => match ptype {
            PrimitiveType::String => value.as_str().map(|s| Scalar::String(s.to_string())),
            PrimitiveType::Long => value
                .as_i64()
                .or_else(|| value.as_u64().map(|u| u as i64))
                .map(Scalar::Long),
            PrimitiveType::Integer => value
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(Scalar::Integer),
            PrimitiveType::Short => value
                .as_i64()
                .and_then(|v| i16::try_from(v).ok())
                .map(Scalar::Short),
            PrimitiveType::Byte => value
                .as_i64()
                .and_then(|v| i8::try_from(v).ok())
                .map(Scalar::Byte),
            PrimitiveType::Float => value.as_f64().map(|f| Scalar::Float(f as f32)),
            PrimitiveType::Double => value.as_f64().map(Scalar::Double),
            PrimitiveType::Boolean => value.as_bool().map(Scalar::Boolean),
            PrimitiveType::Date => value.as_str().and_then(|s| {
                // Parse date string "YYYY-MM-DD" to days since epoch
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .and_then(|date| {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
                        Some(Scalar::Date((date - epoch).num_days() as i32))
                    })
            }),
            PrimitiveType::Timestamp => value.as_str().and_then(|s| {
                // Parse timestamp string to microseconds since epoch
                chrono::DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|dt| Scalar::Timestamp(dt.timestamp_micros()))
            }),
            PrimitiveType::TimestampNtz => value.as_str().and_then(|s| {
                // Parse timestamp without timezone
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                    .ok()
                    .or_else(|| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok())
                    .map(|dt| Scalar::TimestampNtz(dt.and_utc().timestamp_micros()))
            }),
            PrimitiveType::Binary | PrimitiveType::Decimal(..) => None, // Not supported in JSON stats
        },
        _ => None, // Complex types not supported in min/max stats
    }
}

/// Builds a content_stats StructData for a single column from Delta JSON stats.
///
/// Creates a struct with the stats schema fields (value_count, null_count,
/// lower_bound, upper_bound, exact_bounds) populated from the Delta JSON stats.
///
/// # Arguments
/// * `field` - The table schema field for this column
/// * `stats_struct` - The stats schema for this column
/// * `num_records` - The number of records (value_count)
/// * `min_value` - The minimum value (lower_bound)
/// * `max_value` - The maximum value (upper_bound)
/// * `null_count` - The count of null values
/// * `tight_bounds` - Whether the bounds are tight/exact (from Delta's `tightBounds` field)
fn build_column_stats(
    field: &StructField,
    stats_struct: &StructType,
    num_records: Option<i64>,
    min_value: Option<&JsonValue>,
    max_value: Option<&JsonValue>,
    null_count: Option<i64>,
    tight_bounds: bool,
) -> StructData {
    let fields: Vec<StructField> = stats_struct.fields().cloned().collect();
    let mut values: Vec<Scalar> = Vec::with_capacity(fields.len());

    for stats_field in &fields {
        let scalar = match stats_field.name().as_str() {
            "value_count" => num_records.map(Scalar::Long),
            field if field == crate::content_tree::NULL_COUNT_FIELD_NAME => {
                null_count.map(Scalar::Long)
            }
            "nan_value_count" => None, // Not available in Delta JSON stats
            "avg_value_size" => None,  // Not available in Delta JSON stats
            "max_value_size" => None,  // Not available in Delta JSON stats
            "lower_bound" => min_value.and_then(|v| json_value_to_scalar(v, field.data_type())),
            "upper_bound" => max_value.and_then(|v| json_value_to_scalar(v, field.data_type())),
            "exact_bounds" => {
                // exact_bounds reflects Delta's tightBounds field:
                // - true: bounds are exact (all rows in file satisfy min <= value <= max)
                // - false: bounds may be wider (e.g., deletion vectors have removed some rows)
                // Always report this field - defaults to true in Delta JSON when absent
                Some(Scalar::Boolean(tight_bounds))
            }
            _ => None,
        };

        // Use null for missing values
        let scalar = scalar.unwrap_or_else(|| Scalar::Null(stats_field.data_type().clone()));
        values.push(scalar);
    }

    // SAFETY: We've constructed values to match fields in count and type
    StructData::new_unchecked(fields, values)
}

/// Recursively builds content_stats StructData for a struct field.
fn build_struct_stats(
    table_struct: &StructType,
    stats_struct: &StructType,
    delta_stats: &DeltaJsonStats,
    prefix: &str,
) -> StructData {
    let fields: Vec<StructField> = stats_struct.fields().cloned().collect();
    let mut values: Vec<Scalar> = Vec::with_capacity(fields.len());

    for stats_field in &fields {
        let column_name = if prefix.is_empty() {
            stats_field.name().to_string()
        } else {
            format!("{}.{}", prefix, stats_field.name())
        };

        // Find the corresponding field in the table schema
        let table_field = table_struct.field(stats_field.name());

        // Determine if this is a nested struct in the table schema or a primitive column
        // The stats schema always wraps columns in a Struct (containing value_count, bounds, etc.)
        // So we need to check the TABLE schema to know if we should recurse
        let scalar = if let Some(tf) = table_field {
            match tf.data_type() {
                DataType::Struct(nested_table_struct) => {
                    // Table field is a nested struct - recurse into it
                    if let DataType::Struct(nested_stats_struct) = stats_field.data_type() {
                        let nested_data = build_struct_stats(
                            nested_table_struct,
                            nested_stats_struct,
                            delta_stats,
                            &column_name,
                        );
                        Scalar::Struct(nested_data)
                    } else {
                        Scalar::Null(stats_field.data_type().clone())
                    }
                }
                _ => {
                    // Table field is a primitive - build column stats
                    if let DataType::Struct(inner_stats) = stats_field.data_type() {
                        let column_stats = build_column_stats(
                            tf,
                            inner_stats,
                            delta_stats.num_records,
                            delta_stats.min_values.get(&column_name),
                            delta_stats.max_values.get(&column_name),
                            delta_stats.null_count.get(&column_name).copied(),
                            delta_stats.tight_bounds,
                        );
                        Scalar::Struct(column_stats)
                    } else {
                        Scalar::Null(stats_field.data_type().clone())
                    }
                }
            }
        } else {
            Scalar::Null(stats_field.data_type().clone())
        };

        values.push(scalar);
    }

    // SAFETY: We've constructed values to match fields in count and type
    StructData::new_unchecked(fields, values)
}

/// Aggregates multiple content_stats into a single content_stats for a manifest.
///
/// TODO: This should be moved to the engine, because it's more efficient to do this there.
///
/// This function merges statistics from multiple data file entries into aggregate
/// statistics suitable for a manifest entry. It supports both Delta JSON format and
/// AMT-style format.
///
/// For Delta JSON format (with numRecords, minValues, maxValues, nullCount, tightBounds):
/// - `numRecords`: sum of all numRecords
/// - `nullCount`: for each column, sum of null counts
/// - `minValues`: for each column, min of all min values
/// - `maxValues`: for each column, max of all max values
/// - `tightBounds`: AND of all tightBounds (false if any is false)
///
/// For AMT-style format (per-column stats with lower_bound, upper_bound, etc.):
/// - `value_count`: sum of all value_counts
/// - `null_value_count`: sum of all null_value_counts
/// - `nan_value_count`: sum of all nan_value_counts
/// - `avg_value_size`: set to null (would require weighted average calculation)
/// - `max_value_size`: max of all max_value_sizes
/// - `lower_bound`: min of all lower_bounds
/// - `upper_bound`: max of all upper_bounds
/// - `exact_bounds`: AND of all exact_bounds (false if any is false)
///
/// # Arguments
///
/// * `stats_list` - Iterator of Option<&StructData> representing content_stats from multiple entries
///
/// # Returns
///
/// Returns `Some(StructData)` with aggregated statistics if at least one input has stats,
/// or `None` if all inputs are `None`.
pub(crate) fn aggregate_content_stats<'a>(
    stats_list: impl Iterator<Item = Option<&'a StructData>>,
) -> Option<StructData> {
    // Collect non-None stats
    let stats_vec: Vec<&StructData> = stats_list.flatten().collect();

    if stats_vec.is_empty() {
        return None;
    }

    // Use the first stats as a template for the schema
    let template = stats_vec[0];

    // Check if this is Delta JSON format (has numRecords field)
    let is_delta_json_format = template.fields().iter().any(|f| f.name() == "numRecords");

    if is_delta_json_format {
        aggregate_delta_json_stats(&stats_vec)
    } else {
        aggregate_amt_stats(&stats_vec)
    }
}

/// Aggregates Delta JSON format stats (numRecords, minValues, maxValues, nullCount, tightBounds)
fn aggregate_delta_json_stats(stats_vec: &[&StructData]) -> Option<StructData> {
    let template = stats_vec[0];
    let fields: Vec<StructField> = template.fields().to_vec();
    let mut aggregated_values: Vec<Scalar> = Vec::with_capacity(fields.len());

    for (field_idx, field) in fields.iter().enumerate() {
        let field_name = field.name().as_str();
        let field_values: Vec<&Scalar> = stats_vec.iter().map(|s| &s.values()[field_idx]).collect();

        let aggregated = match field_name {
            "numRecords" => sum_long_scalars(&field_values),
            "tightBounds" => and_boolean_scalars(&field_values),
            "nullCount" => aggregate_struct_by_sum(&field_values, field.data_type()),
            "minValues" => aggregate_struct_by_min(&field_values, field.data_type()),
            "maxValues" => aggregate_struct_by_max(&field_values, field.data_type()),
            _ => Scalar::Null(field.data_type().clone()),
        };
        aggregated_values.push(aggregated);
    }

    Some(StructData::new_unchecked(fields, aggregated_values))
}

/// Aggregates AMT-style stats (per-column stats with lower_bound, upper_bound, etc.)
fn aggregate_amt_stats(stats_vec: &[&StructData]) -> Option<StructData> {
    let template = stats_vec[0];
    let fields: Vec<StructField> = template.fields().to_vec();
    let mut aggregated_values: Vec<Scalar> = Vec::with_capacity(fields.len());

    for (field_idx, field) in fields.iter().enumerate() {
        let field_values: Vec<&Scalar> = stats_vec.iter().map(|s| &s.values()[field_idx]).collect();
        let aggregated = aggregate_column_stats(field, &field_values);
        aggregated_values.push(aggregated);
    }

    Some(StructData::new_unchecked(fields, aggregated_values))
}

/// Aggregates a struct field by summing all Long values for each nested field
fn aggregate_struct_by_sum(values: &[&Scalar], data_type: &DataType) -> Scalar {
    let struct_values: Vec<&StructData> = values
        .iter()
        .filter_map(|v| match v {
            Scalar::Struct(s) => Some(s),
            _ => None,
        })
        .collect();

    if struct_values.is_empty() {
        return Scalar::Null(data_type.clone());
    }

    let template = struct_values[0];
    let fields: Vec<StructField> = template.fields().to_vec();
    let mut aggregated_values: Vec<Scalar> = Vec::with_capacity(fields.len());

    for (field_idx, _field) in fields.iter().enumerate() {
        let field_values: Vec<&Scalar> = struct_values
            .iter()
            .map(|s| &s.values()[field_idx])
            .collect();
        aggregated_values.push(sum_long_scalars(&field_values));
    }

    Scalar::Struct(StructData::new_unchecked(fields, aggregated_values))
}

/// Aggregates a struct field by taking the min for each nested field
fn aggregate_struct_by_min(values: &[&Scalar], data_type: &DataType) -> Scalar {
    let struct_values: Vec<&StructData> = values
        .iter()
        .filter_map(|v| match v {
            Scalar::Struct(s) => Some(s),
            _ => None,
        })
        .collect();

    if struct_values.is_empty() {
        return Scalar::Null(data_type.clone());
    }

    let template = struct_values[0];
    let fields: Vec<StructField> = template.fields().to_vec();
    let mut aggregated_values: Vec<Scalar> = Vec::with_capacity(fields.len());

    for (field_idx, field) in fields.iter().enumerate() {
        let field_values: Vec<&Scalar> = struct_values
            .iter()
            .map(|s| &s.values()[field_idx])
            .collect();
        aggregated_values.push(min_scalar(&field_values, field.data_type()));
    }

    Scalar::Struct(StructData::new_unchecked(fields, aggregated_values))
}

/// Aggregates a struct field by taking the max for each nested field
fn aggregate_struct_by_max(values: &[&Scalar], data_type: &DataType) -> Scalar {
    let struct_values: Vec<&StructData> = values
        .iter()
        .filter_map(|v| match v {
            Scalar::Struct(s) => Some(s),
            _ => None,
        })
        .collect();

    if struct_values.is_empty() {
        return Scalar::Null(data_type.clone());
    }

    let template = struct_values[0];
    let fields: Vec<StructField> = template.fields().to_vec();
    let mut aggregated_values: Vec<Scalar> = Vec::with_capacity(fields.len());

    for (field_idx, field) in fields.iter().enumerate() {
        let field_values: Vec<&Scalar> = struct_values
            .iter()
            .map(|s| &s.values()[field_idx])
            .collect();
        aggregated_values.push(max_scalar(&field_values, field.data_type()));
    }

    Scalar::Struct(StructData::new_unchecked(fields, aggregated_values))
}

/// Aggregates statistics for a single column across multiple entries.
///
/// If the column's stats are themselves structs (either nested table column or primitive stats),
/// this function determines whether to recurse for nested columns or aggregate primitive stats.
fn aggregate_column_stats(field: &StructField, values: &[&Scalar]) -> Scalar {
    // Check if this is a stats struct (has lower_bound/upper_bound fields) or a nested column
    match field.data_type() {
        DataType::Struct(inner_struct) => {
            // Check if this looks like a primitive stats struct (has lower_bound field)
            if inner_struct.field("lower_bound").is_some() {
                // This is a primitive column's stats struct - aggregate the stats fields
                aggregate_primitive_stats(inner_struct.as_ref(), values)
            } else {
                // This is a nested table column - recurse into its fields
                let inner_stats: Vec<&StructData> = values
                    .iter()
                    .filter_map(|v| match v {
                        Scalar::Struct(s) => Some(s),
                        _ => None,
                    })
                    .collect();

                if inner_stats.is_empty() {
                    return Scalar::Null(field.data_type().clone());
                }

                let inner_fields: Vec<StructField> = inner_struct.fields().cloned().collect();
                let mut inner_aggregated: Vec<Scalar> = Vec::with_capacity(inner_fields.len());

                for (idx, inner_field) in inner_fields.iter().enumerate() {
                    let inner_values: Vec<&Scalar> =
                        inner_stats.iter().map(|s| &s.values()[idx]).collect();
                    inner_aggregated.push(aggregate_column_stats(inner_field, &inner_values));
                }

                Scalar::Struct(StructData::new_unchecked(inner_fields, inner_aggregated))
            }
        }
        _ => {
            // Not a struct, shouldn't happen in well-formed stats
            Scalar::Null(field.data_type().clone())
        }
    }
}

/// Aggregates primitive column stats fields (value_count, lower_bound, etc.)
fn aggregate_primitive_stats(stats_struct: &StructType, values: &[&Scalar]) -> Scalar {
    // Extract the inner StructData from each Scalar::Struct
    let struct_values: Vec<&StructData> = values
        .iter()
        .filter_map(|v| match v {
            Scalar::Struct(s) => Some(s),
            _ => None,
        })
        .collect();

    if struct_values.is_empty() {
        return Scalar::Null(DataType::Struct(Box::new(stats_struct.clone())));
    }

    let fields: Vec<StructField> = stats_struct.fields().cloned().collect();
    let mut aggregated_values: Vec<Scalar> = Vec::with_capacity(fields.len());

    for (field_idx, field) in fields.iter().enumerate() {
        let field_name = field.name().as_str();
        let field_scalars: Vec<&Scalar> = struct_values
            .iter()
            .map(|s| &s.values()[field_idx])
            .collect();

        let aggregated = match field_name {
            // Sum fields
            "value_count" | "nan_value_count" => sum_long_scalars(&field_scalars),
            field if field == crate::content_tree::NULL_COUNT_FIELD_NAME => {
                sum_long_scalars(&field_scalars)
            }
            // Max fields
            "max_value_size" => max_scalar(&field_scalars, &DataType::INTEGER),
            // Min bound
            "lower_bound" => min_scalar(&field_scalars, field.data_type()),
            // Max bound
            "upper_bound" => max_scalar(&field_scalars, field.data_type()),
            // AND of all exact_bounds
            "exact_bounds" => and_boolean_scalars(&field_scalars),
            // Skip avg_value_size (would need weighted average, not straightforward)
            "avg_value_size" => Scalar::Null(field.data_type().clone()),
            // Unknown field - preserve as null
            _ => Scalar::Null(field.data_type().clone()),
        };

        aggregated_values.push(aggregated);
    }

    Scalar::Struct(StructData::new_unchecked(fields, aggregated_values))
}

/// Sums Long scalars, ignoring nulls.
fn sum_long_scalars(scalars: &[&Scalar]) -> Scalar {
    let mut sum: i64 = 0;
    let mut has_value = false;

    for scalar in scalars {
        if let Scalar::Long(v) = scalar {
            sum += v;
            has_value = true;
        }
    }

    if has_value {
        Scalar::Long(sum)
    } else {
        Scalar::Null(DataType::LONG)
    }
}

/// Takes the minimum scalar value, ignoring nulls.
fn min_scalar(scalars: &[&Scalar], data_type: &DataType) -> Scalar {
    let mut min: Option<&Scalar> = None;

    for scalar in scalars {
        if scalar.is_null() {
            continue;
        }
        min = Some(match min {
            None => scalar,
            Some(current_min) => {
                match current_min.logical_partial_cmp(scalar) {
                    Some(std::cmp::Ordering::Greater) => scalar, // scalar < current_min
                    _ => current_min,
                }
            }
        });
    }

    min.cloned()
        .unwrap_or_else(|| Scalar::Null(data_type.clone()))
}

/// Takes the maximum scalar value, ignoring nulls.
fn max_scalar(scalars: &[&Scalar], data_type: &DataType) -> Scalar {
    let mut max: Option<&Scalar> = None;

    for scalar in scalars {
        if scalar.is_null() {
            continue;
        }
        max = Some(match max {
            None => scalar,
            Some(current_max) => {
                match current_max.logical_partial_cmp(scalar) {
                    Some(std::cmp::Ordering::Less) => scalar, // scalar > current_max
                    _ => current_max,
                }
            }
        });
    }

    max.cloned()
        .unwrap_or_else(|| Scalar::Null(data_type.clone()))
}

/// ANDs boolean scalars. Returns false if any is false, true if all are true, null otherwise.
fn and_boolean_scalars(scalars: &[&Scalar]) -> Scalar {
    let mut result: Option<bool> = None;

    for scalar in scalars {
        match scalar {
            Scalar::Boolean(false) => return Scalar::Boolean(false), // Short-circuit on false
            Scalar::Boolean(true) => result = Some(result.unwrap_or(true)),
            _ => {} // Ignore nulls
        }
    }

    result
        .map(Scalar::Boolean)
        .unwrap_or_else(|| Scalar::Null(DataType::BOOLEAN))
}

/// Converts Delta Protocol JSON stats to content_stats StructData format.
///
/// This function takes the raw JSON stats string from a Delta Add action and converts
/// it to the StructData format expected by the content_stats field in ContentTreeNodeEntry.
///
/// The output format is the Delta JSON stats format:
/// - numRecords: LONG
/// - nullCount: struct with each column as LONG
/// - minValues: struct with each column keeping its original type
/// - maxValues: struct with each column keeping its original type
/// - tightBounds: BOOLEAN
///
/// # Arguments
///
/// * `stats_json` - The JSON stats string from the Add action's `stats` field
/// * `table_schema` - The table's data schema (used to determine column types)
///
/// # Returns
///
/// Returns `Ok(Some(StructData))` if conversion succeeds, `Ok(None)` if stats_json is None or
/// cannot be parsed.
///
/// # Example
///
/// ```ignore
/// let stats_json = r#"{"numRecords":100,"minValues":{"id":1},"maxValues":{"id":100},"nullCount":{"id":0}}"#;
/// let content_stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)?;
/// ```
pub(crate) fn delta_json_stats_to_content_stats(
    stats_json: Option<&str>,
    table_schema: &StructType,
) -> DeltaResult<Option<StructData>> {
    let Some(json_str) = stats_json else {
        return Ok(None);
    };

    let Some(delta_stats) = DeltaJsonStats::parse(json_str) else {
        return Ok(None);
    };

    // Generate the AMT-style stats schema
    let stats_struct = stats_schema(table_schema)?;

    // Build the content_stats StructData in AMT format
    let content_stats = build_struct_stats(table_schema, &stats_struct, &delta_stats, "");

    Ok(Some(content_stats))
}

/// Checks if a schema's stats column is in Delta JSON format (has numRecords).
fn is_delta_json_stats_schema(schema: &StructType, stats_column_name: &str) -> bool {
    schema
        .field(stats_column_name)
        .and_then(|f| match f.data_type() {
            DataType::Struct(s) => Some(s),
            _ => None,
        })
        .is_some_and(|s| s.field("numRecords").is_some())
}

/// Builds a Transform expression that replaces a Delta JSON stats column with AMT format.
/// Returns (expression, amt_stats_schema).
///
/// When `known_stats_schema` is `None`, the expression assumes all Delta JSON stat fields
/// exist in the data (numRecords, minValues, maxValues, nullCount, tightBounds). When
/// `Some`, only fields present in the known schema generate column references; others
/// become null literals.
pub(crate) fn build_delta_to_amt_pivot_expression(
    table_schema: &StructType,
    stats_column_name: &str,
    known_stats_schema: Option<&StructType>,
) -> DeltaResult<(Expression, StructType)> {
    let amt_stats_schema = stats_schema(table_schema)?;
    let amt_struct_expr = build_amt_struct_expr(
        table_schema,
        &amt_stats_schema,
        stats_column_name,
        &[],
        known_stats_schema,
    );

    // Use Transform to replace the stats_parsed field while preserving other fields
    let transform = Expression::transform(
        Transform::new_top_level()
            .with_replaced_field(stats_column_name, Arc::new(amt_struct_expr)),
    );
    Ok((transform, amt_stats_schema))
}

/// Builds a struct expression for one level of the AMT hierarchy.
///
/// Walks `table_schema` and `amt_schema` together. For struct fields it recurses;
/// for primitive fields it calls [`build_amt_leaf_expr`].
fn build_amt_struct_expr(
    table_schema: &StructType,
    amt_schema: &StructType,
    stats_col: &str,
    col_path: &[&str],
    known_stats_schema: Option<&StructType>,
) -> Expression {
    let exprs: Vec<ExpressionRef> = amt_schema
        .fields()
        .map(|amt_field| {
            let table_field = table_schema.field(amt_field.name());
            let expr = if let Some(tf) = table_field {
                match tf.data_type() {
                    DataType::Struct(nested_table_struct) => {
                        let nested_amt_struct = match amt_field.data_type() {
                            DataType::Struct(s) => s.as_ref(),
                            _ => unreachable!("AMT schema for struct field must be struct"),
                        };
                        let mut new_path = col_path.to_vec();
                        new_path.push(amt_field.name().as_str());
                        build_amt_struct_expr(
                            nested_table_struct,
                            nested_amt_struct,
                            stats_col,
                            &new_path,
                            known_stats_schema,
                        )
                    }
                    _ => {
                        let amt_leaf_struct = match amt_field.data_type() {
                            DataType::Struct(s) => s.as_ref(),
                            _ => unreachable!("AMT schema for primitive field must be struct"),
                        };
                        let mut leaf_path = col_path.to_vec();
                        leaf_path.push(amt_field.name().as_str());
                        build_amt_leaf_expr(
                            amt_leaf_struct,
                            stats_col,
                            &leaf_path,
                            known_stats_schema,
                        )
                    }
                }
            } else {
                Expression::null_literal(amt_field.data_type().clone())
            };
            Arc::new(expr)
        })
        .collect();

    Expression::Struct(exprs, Some(Box::new(amt_schema.clone())), None)
}

/// Checks if a nested field path exists in a struct type.
/// For example, `has_nested_field(schema, &["minValues", "id"])` checks if
/// `schema.minValues.id` exists.
fn has_nested_field(schema: &StructType, path: &[&str]) -> bool {
    match path {
        [] => true,
        [first, rest @ ..] => match schema.field(*first) {
            Some(f) => {
                if rest.is_empty() {
                    true
                } else {
                    match f.data_type() {
                        DataType::Struct(s) => has_nested_field(s, rest),
                        _ => false,
                    }
                }
            }
            None => false,
        },
    }
}

/// Builds per-column stats struct expression for a leaf (primitive) column.
///
/// The field ordering matches the AMT stats schema from [`stats_schema`]:
/// value_count, null_value_count, nan_value_count, avg_value_size,
/// max_value_size, lower_bound, upper_bound, exact_bounds.
///
/// When `known_stats_schema` is `Some`, column references are only created for fields
/// that exist in the known schema. When `None`, all Delta JSON fields are assumed to exist.
fn build_amt_leaf_expr(
    amt_leaf_schema: &StructType,
    stats_col: &str,
    col_path: &[&str],
    known_stats_schema: Option<&StructType>,
) -> Expression {
    // Helper: check if a Delta JSON field path exists. When known_stats_schema is None,
    // assume all fields exist (optimistic mode).
    let field_exists = |delta_field: &str, nested_path: &[&str]| -> bool {
        match known_stats_schema {
            None => true,
            Some(schema) => {
                let mut full_path = vec![delta_field];
                full_path.extend_from_slice(nested_path);
                has_nested_field(schema, &full_path)
            }
        }
    };

    let exprs: Vec<ExpressionRef> = amt_leaf_schema
        .fields()
        .map(|field| {
            let expr = match field.name().as_str() {
                "value_count" if field_exists("numRecords", &[]) => {
                    Expression::column([stats_col, "numRecords"])
                }
                name if name == NULL_COUNT_FIELD_NAME && field_exists("nullCount", col_path) => {
                    let mut path: Vec<&str> = vec![stats_col, "nullCount"];
                    path.extend_from_slice(col_path);
                    Expression::column(path)
                }
                "lower_bound" if field_exists("minValues", col_path) => {
                    let mut path: Vec<&str> = vec![stats_col, "minValues"];
                    path.extend_from_slice(col_path);
                    Expression::column(path)
                }
                "upper_bound" if field_exists("maxValues", col_path) => {
                    let mut path: Vec<&str> = vec![stats_col, "maxValues"];
                    path.extend_from_slice(col_path);
                    Expression::column(path)
                }
                "exact_bounds" if field_exists("tightBounds", &[]) => {
                    Expression::column([stats_col, "tightBounds"])
                }
                // Field not in Delta JSON stats or not present in known schema → null literal
                _ => Expression::null_literal(field.data_type().clone()),
            };
            Arc::new(expr)
        })
        .collect();

    Expression::Struct(exprs, Some(Box::new(amt_leaf_schema.clone())), None)
}

/// Engine-agnostic pre-conversion of a stats column from Delta JSON to AMT format.
///
/// Returns `Ok(Some(converted_data))` if conversion succeeded, `Ok(None)` if the data
/// doesn't appear to have Delta JSON format stats (e.g., empty stats, already AMT format,
/// or the stats column is missing).
pub(crate) fn try_pre_convert_stats_column(
    engine: &dyn Engine,
    data: &dyn EngineData,
    stats_column_name: &str,
    table_schema: &StructType,
    input_schema: &StructType,
) -> DeltaResult<Option<Box<dyn EngineData>>> {
    // Quick check: does the known schema suggest Delta JSON format?
    if !is_delta_json_stats_schema(input_schema, stats_column_name) {
        return Ok(None);
    }

    // Extract the input stats struct schema for conservative field-existence checks
    let input_stats_struct =
        input_schema
            .field(stats_column_name)
            .and_then(|f| match f.data_type() {
                DataType::Struct(s) => Some(s.as_ref().clone()),
                _ => None,
            });

    // Helper to build output schema and evaluator for a given expression
    let build_evaluator = |expr: Expression,
                           amt_stats_schema: &StructType|
     -> DeltaResult<Arc<dyn crate::ExpressionEvaluator>> {
        let output_fields: Vec<StructField> = input_schema
            .fields()
            .map(|f| {
                if f.name() == stats_column_name {
                    StructField::new(
                        f.name(),
                        DataType::Struct(Box::new(amt_stats_schema.clone())),
                        f.nullable,
                    )
                } else {
                    f.clone()
                }
            })
            .collect();
        let output_schema = StructType::new_unchecked(output_fields);
        engine.evaluation_handler().new_expression_evaluator(
            Arc::new(input_schema.clone()),
            Arc::new(expr),
            DataType::Struct(Box::new(output_schema)),
        )
    };

    // Step 1: Try optimistic conversion assuming all Delta JSON fields exist in the data.
    // The actual data may have more fields than input_schema declares.
    // This code should now be unreachable due to the Map type check above,
    // but keeping it for potential future use with non-Map schemas
    let (expr, amt_stats_schema) =
        build_delta_to_amt_pivot_expression(table_schema, stats_column_name, None)?;
    let evaluator = build_evaluator(expr, &amt_stats_schema)?;
    match evaluator.evaluate(data) {
        Ok(result) => {
            return Ok(Some(result));
        }
        Err(_e) => {
            // Optimistic conversion failed, try conservative approach
        }
    }

    // Step 2: If optimistic evaluation failed (e.g., data only has numRecords),
    // try a conservative approach using only fields declared in input_schema.
    if let Some(ref known_schema) = input_stats_struct {
        let (expr, amt_stats_schema) = build_delta_to_amt_pivot_expression(
            table_schema,
            stats_column_name,
            Some(known_schema),
        )?;
        let evaluator = build_evaluator(expr, &amt_stats_schema)?;
        match evaluator.evaluate(data) {
            Ok(result) => {
                return Ok(Some(result));
            }
            Err(_e) => {
                // Conservative conversion also failed
            }
        }
    }

    // Return None means conversion failed - caller will use NULL for content_stats
    // This will result in manifest-level data skipping being unable to filter manifests
    Ok(None)
}

/// Extracts a named field from a struct schema as a `&StructType`, returning `None` if absent
/// or if the field is not a struct type.
fn get_struct_sub_schema<'a>(schema: &'a StructType, field_name: &str) -> Option<&'a StructType> {
    schema.field(field_name).and_then(|f| match f.data_type() {
        DataType::Struct(s) => Some(s.as_ref()),
        _ => None,
    })
}

/// Builds an AMT-format stats sub-struct for a single primitive column, containing only the
/// fields needed to read the requested Delta stat types.
///
/// Currently includes all AMT sub-fields for the column (matching `build_primitive_stats_struct`
/// for the given nullability), using the `nullable` parameter derived from whether the column
/// appears in the Delta `nullCount` sub-schema.
///
/// TODO: Further filter sub-fields based on which Delta stat types are requested. E.g. if only
/// `minValues` is requested, omit `null_value_count` and `upper_bound` from the read schema.
fn build_primitive_amt_struct_for_stats(
    base_field_id: i32,
    data_type: &DataType,
    nullable: bool,
    _needs_min: bool,
    _needs_max: bool,
) -> StructType {
    build_primitive_stats_struct(base_field_id, data_type.clone(), nullable)
}

/// Generates AMT-format stats schema fields for only the (stat type, column) pairs present in
/// the provided Delta stat column schemas. Each column only gets the AMT sub-fields needed to
/// read the Delta stat types where it appears.
fn filtered_stats_schema_fields(
    table_schema: &StructType,
    null_count_cols: Option<&StructType>,
    min_vals_cols: Option<&StructType>,
    max_vals_cols: Option<&StructType>,
) -> DeltaResult<Vec<StructField>> {
    // Collect unique column names across all three stat categories
    let mut col_names: Vec<&str> = Vec::new();
    for schema in [null_count_cols, min_vals_cols, max_vals_cols]
        .into_iter()
        .flatten()
    {
        for field in schema.fields() {
            let name = field.name().as_str();
            if !col_names.contains(&name) {
                col_names.push(name);
            }
        }
    }

    let mut fields = Vec::new();
    for col_name in col_names {
        let Some(table_field) = table_schema.field(col_name) else {
            continue;
        };
        let field_id = get_field_id(table_field).ok_or_else(|| {
            crate::Error::generic(format!(
                "Field '{}' is missing field ID! ContentTreeNode: {:#?}",
                table_field.name(),
                table_field.metadata()
            ))
        })?;
        let base_stats_id = field_id_to_statistics_base(field_id).ok_or_else(|| {
            crate::Error::generic(format!(
                "Failed to compute stats field ID for field '{}' with id {}",
                table_field.name(),
                field_id
            ))
        })?;

        let stats_struct = match table_field.data_type() {
            DataType::Primitive(_) => {
                let needs_min = min_vals_cols.is_some_and(|s| s.field(col_name).is_some());
                let needs_max = max_vals_cols.is_some_and(|s| s.field(col_name).is_some());
                build_primitive_amt_struct_for_stats(
                    base_stats_id,
                    table_field.data_type(),
                    table_field.nullable,
                    needs_min,
                    needs_max,
                )
            }
            DataType::Struct(table_nested) => {
                let nc_nested = null_count_cols.and_then(|s| get_struct_sub_schema(s, col_name));
                let min_nested = min_vals_cols.and_then(|s| get_struct_sub_schema(s, col_name));
                let max_nested = max_vals_cols.and_then(|s| get_struct_sub_schema(s, col_name));
                StructType::new_unchecked(filtered_stats_schema_fields(
                    table_nested,
                    nc_nested,
                    min_nested,
                    max_nested,
                )?)
            }
            _ => continue,
        };

        let metadata: Vec<(&str, MetadataValue)> = table_field
            .metadata
            .iter()
            .map(|(k, v)| {
                let value = if k == ColumnMetadataKey::ParquetFieldId.as_ref()
                    || k == ColumnMetadataKey::ColumnMappingId.as_ref()
                {
                    MetadataValue::Number(base_stats_id as i64)
                } else {
                    v.clone()
                };
                (k.as_str(), value)
            })
            .collect();
        fields.push(
            StructField::new(
                table_field.name(),
                DataType::Struct(Box::new(stats_struct)),
                true,
            )
            .with_metadata(metadata),
        );
    }
    Ok(fields)
}

/// Generates AMT-format content_stats schema containing only the (stat type, column) pairs
/// present in `stats_schema`. Each column gets only the AMT sub-fields needed for the Delta
/// stat types where it appears, avoiding wasteful reads of unused per-column statistics.
pub(crate) fn filtered_stats_schema(
    table_schema: &StructType,
    stats_schema: &StructType,
) -> DeltaResult<StructType> {
    let fields = filtered_stats_schema_fields(
        table_schema,
        get_struct_sub_schema(stats_schema, "nullCount"),
        get_struct_sub_schema(stats_schema, "minValues"),
        get_struct_sub_schema(stats_schema, "maxValues"),
    )?;
    Ok(StructType::new_unchecked(fields))
}

pub(crate) fn create_content_stats_to_stats_parsed_expr(
    table_schema: &StructType,
    stats_schema: &StructType,
) -> DeltaResult<ExpressionRef> {
    // Build a map from physical column names (with "." for nested) to field IDs
    // These are the physical names as they appear in the provided schema
    let mut column_to_field_id: HashMap<String, i32> = HashMap::new();
    build_column_to_field_id_map(table_schema, "", &mut column_to_field_id)?;

    // If no columns have field IDs, return early
    if column_to_field_id.is_empty() {
        return Err(crate::Error::generic(
            "No fields with field IDs found in table schema",
        ));
    }

    // Build expressions for each stat type independently, using only the columns present
    // in each Delta stat category (nullCount, minValues, maxValues).
    let null_count_cols = get_struct_sub_schema(stats_schema, "nullCount");
    let min_vals_cols = get_struct_sub_schema(stats_schema, "minValues");
    let max_vals_cols = get_struct_sub_schema(stats_schema, "maxValues");

    let mut null_count_exprs = Vec::new();
    let mut min_values_exprs = Vec::new();
    let mut max_values_exprs = Vec::new();
    let mut null_count_fields = Vec::new();
    let mut min_max_fields = Vec::new();

    collect_stats_expressions_filtered(
        table_schema,
        null_count_cols,
        min_vals_cols,
        max_vals_cols,
        "",
        &column_to_field_id,
        &mut null_count_exprs,
        &mut min_values_exprs,
        &mut max_values_exprs,
        &mut null_count_fields,
        &mut min_max_fields,
    )?;

    // Build the stats_parsed struct schema
    let null_count_schema = StructType::new_unchecked(null_count_fields);
    let min_max_schema = StructType::new_unchecked(min_max_fields);

    // Build nested struct expressions
    let null_count_struct = if null_count_exprs.is_empty() {
        Expression::literal(Scalar::Null(DataType::Struct(Box::new(
            StructType::new_unchecked(Vec::new()),
        ))))
    } else {
        Expression::struct_from_with_schema(null_count_exprs, null_count_schema.clone())
    };

    let min_values_struct = if min_values_exprs.is_empty() {
        Expression::literal(Scalar::Null(DataType::Struct(Box::new(
            StructType::new_unchecked(Vec::new()),
        ))))
    } else {
        Expression::struct_from_with_schema(min_values_exprs.clone(), min_max_schema.clone())
    };

    let max_values_struct = if max_values_exprs.is_empty() {
        Expression::literal(Scalar::Null(DataType::Struct(Box::new(
            StructType::new_unchecked(Vec::new()),
        ))))
    } else {
        Expression::struct_from_with_schema(max_values_exprs, min_max_schema.clone())
    };

    // Project numRecords from the manifest entry's recordCount field
    // This expression will be evaluated against the manifest batch data which includes recordCount
    let num_records_expr = Expression::column(["recordCount"]);

    // Build the final stats_parsed struct
    let stats_parsed_schema = StructType::new_unchecked(vec![
        StructField::nullable("numRecords", DataType::LONG),
        StructField::nullable("nullCount", DataType::Struct(Box::new(null_count_schema))),
        StructField::nullable(
            "minValues",
            DataType::Struct(Box::new(min_max_schema.clone())),
        ),
        StructField::nullable("maxValues", DataType::Struct(Box::new(min_max_schema))),
    ]);

    Ok(Arc::new(Expression::struct_from_with_schema(
        vec![
            num_records_expr,
            null_count_struct,
            min_values_struct,
            max_values_struct,
        ],
        stats_parsed_schema,
    )))
}

/// Builds a map from physical column names (with "." for nested) to field IDs.
///
/// The column names are taken from the provided schema, which should be the physical schema.
fn build_column_to_field_id_map(
    schema: &StructType,
    prefix: &str,
    map: &mut HashMap<String, i32>,
) -> DeltaResult<()> {
    for field in schema.fields() {
        let field_name = if prefix.is_empty() {
            field.name().to_string()
        } else {
            format!("{}.{}", prefix, field.name())
        };

        // Get field ID from metadata
        if let Some(field_id) = get_field_id(field) {
            map.insert(field_name.clone(), field_id);
        }

        // Recurse into nested structs
        if let DataType::Struct(nested_schema) = field.data_type() {
            build_column_to_field_id_map(nested_schema, &field_name, map)?;
        }
    }
    Ok(())
}

/// Collects AMT→Delta stats transformation expressions for each (stat type, column) pair.
///
/// Each stat category (`null_count_cols`, `min_vals_cols`, `max_vals_cols`) independently
/// controls which columns contribute to its corresponding output expressions/fields.
#[allow(clippy::too_many_arguments)]
fn collect_stats_expressions_filtered(
    table_schema: &StructType,
    null_count_cols: Option<&StructType>,
    min_vals_cols: Option<&StructType>,
    max_vals_cols: Option<&StructType>,
    prefix: &str,
    column_to_field_id: &HashMap<String, i32>,
    null_count_exprs: &mut Vec<ExpressionRef>,
    min_values_exprs: &mut Vec<ExpressionRef>,
    max_values_exprs: &mut Vec<ExpressionRef>,
    null_count_fields: &mut Vec<StructField>,
    min_max_fields: &mut Vec<StructField>,
) -> DeltaResult<()> {
    // Collect unique column names across all three stat categories
    let mut col_names: Vec<&str> = Vec::new();
    for schema in [null_count_cols, min_vals_cols, max_vals_cols]
        .into_iter()
        .flatten()
    {
        for field in schema.fields() {
            let name = field.name().as_str();
            if !col_names.contains(&name) {
                col_names.push(name);
            }
        }
    }

    for col_name in col_names {
        let field_path = if prefix.is_empty() {
            col_name.to_string()
        } else {
            format!("{}.{}", prefix, col_name)
        };

        let Some(table_field) = table_schema.field(col_name) else {
            continue;
        };

        match table_field.data_type() {
            DataType::Struct(table_nested) => {
                let nc_nested = null_count_cols.and_then(|s| get_struct_sub_schema(s, col_name));
                let min_nested = min_vals_cols.and_then(|s| get_struct_sub_schema(s, col_name));
                let max_nested = max_vals_cols.and_then(|s| get_struct_sub_schema(s, col_name));

                let mut nested_null_count_exprs = Vec::new();
                let mut nested_min_values_exprs = Vec::new();
                let mut nested_max_values_exprs = Vec::new();
                let mut nested_null_count_fields = Vec::new();
                let mut nested_min_max_fields = Vec::new();

                collect_stats_expressions_filtered(
                    table_nested,
                    nc_nested,
                    min_nested,
                    max_nested,
                    &field_path,
                    column_to_field_id,
                    &mut nested_null_count_exprs,
                    &mut nested_min_values_exprs,
                    &mut nested_max_values_exprs,
                    &mut nested_null_count_fields,
                    &mut nested_min_max_fields,
                )?;

                if !nested_null_count_exprs.is_empty() {
                    let nested_null_count_schema =
                        StructType::new_unchecked(nested_null_count_fields);
                    null_count_exprs.push(Arc::new(Expression::struct_from_with_schema(
                        nested_null_count_exprs,
                        nested_null_count_schema.clone(),
                    )));
                    null_count_fields.push(StructField::nullable(
                        table_field.name(),
                        DataType::Struct(Box::new(nested_null_count_schema)),
                    ));
                }

                if !nested_min_values_exprs.is_empty() {
                    let nested_min_max_schema = StructType::new_unchecked(nested_min_max_fields);
                    min_values_exprs.push(Arc::new(Expression::struct_from_with_schema(
                        nested_min_values_exprs,
                        nested_min_max_schema.clone(),
                    )));
                    max_values_exprs.push(Arc::new(Expression::struct_from_with_schema(
                        nested_max_values_exprs,
                        nested_min_max_schema.clone(),
                    )));
                    min_max_fields.push(StructField::nullable(
                        table_field.name(),
                        DataType::Struct(Box::new(nested_min_max_schema)),
                    ));
                }
            }
            _ if matches!(table_field.data_type(), DataType::Primitive(_)) => {
                if !column_to_field_id.contains_key(&field_path) {
                    continue;
                }
                if null_count_cols.is_some_and(|s| s.field(col_name).is_some()) {
                    null_count_exprs.push(Arc::new(Expression::Column(ColumnName::new([
                        crate::content_tree::CONTENT_STATS_FIELD_NAME,
                        &field_path,
                        crate::content_tree::NULL_COUNT_FIELD_NAME,
                    ]))));
                    null_count_fields
                        .push(StructField::nullable(table_field.name(), DataType::LONG));
                }
                let has_min = min_vals_cols.is_some_and(|s| s.field(col_name).is_some());
                let has_max = max_vals_cols.is_some_and(|s| s.field(col_name).is_some());
                if has_min {
                    min_values_exprs.push(Arc::new(Expression::Column(ColumnName::new([
                        crate::content_tree::CONTENT_STATS_FIELD_NAME,
                        &field_path,
                        "lower_bound",
                    ]))));
                }
                if has_max {
                    max_values_exprs.push(Arc::new(Expression::Column(ColumnName::new([
                        crate::content_tree::CONTENT_STATS_FIELD_NAME,
                        &field_path,
                        "upper_bound",
                    ]))));
                }
                if has_min || has_max {
                    min_max_fields.push(StructField::nullable(
                        table_field.name(),
                        table_field.data_type().clone(),
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_id_to_statistics_base() {
        // field_id -> expected stats_field_id
        let cases = [
            (0, Some(10_000)),
            (1, Some(10_200)),
            (2, Some(10_400)),
            (5, Some(11_000)),
            (100, Some(30_000)),
            (-1, None),                     // negative
            (1_000_000, Some(200_010_000)), // 1_000_000 = max data field id
            (2_147_483_447, Some(2_147_000_000)),
            (2_147_483_448, Some(2_147_000_200)),
            (2_147_483_541, Some(2_147_018_800)),
            (2_147_483_645, Some(2_147_039_600)),
            (2_147_483_646, Some(2_147_039_800)),
        ];
        for (field_id, expected) in cases {
            assert_eq!(
                field_id_to_statistics_base(field_id),
                expected,
                "Failed for field_id {}",
                field_id
            );
        }
    }

    #[test]
    fn test_statistics_base_to_field_id() {
        // stats_field_id -> expected field_id
        let cases = [
            (10_000, Some(0)),
            (10_200, Some(1)),
            (10_400, Some(2)),
            (11_000, Some(5)),
            (30_000, Some(100)),
            (200_010_000, Some(1_000_000)), // 1_000_000 = max data field id
            (2_147_000_000, Some(2_147_483_447)),
            (2_147_000_200, Some(2_147_483_448)),
            (2_147_018_800, Some(2_147_483_541)),
            (2_147_039_600, Some(2_147_483_645)),
            (2_147_039_800, Some(2_147_483_646)),
            // Invalid cases
            (-1, None), // negative
            // below data space start (would give negative field_id)
            (0, None),
            (5_000, None),
            // not a multiple of 200
            (10_001, None),
            (10_201, None),
            (10_500, None),
            (10_900, None),
        ];
        for (stats_field_id, expected) in cases {
            assert_eq!(
                statistics_base_to_field_id(stats_field_id),
                expected,
                "Failed for stats_field_id {}",
                stats_field_id
            );
        }
    }

    #[test]
    fn test_roundtrip() {
        for field_id in [0, 1, 2, 5, 100, 1000] {
            let stats_id = field_id_to_statistics_base(field_id).unwrap();
            let recovered = statistics_base_to_field_id(stats_id).unwrap();
            assert_eq!(
                field_id, recovered,
                "Roundtrip failed for field_id {}",
                field_id
            );
        }
    }

    #[test]
    fn test_stats_schema_non_nullable_int() {
        // Non-nullable integer field: should have 4 stats fields
        // (no null_value_count, no nan_value_count, no size stats)
        let field = field_with_id("id", DataType::INTEGER, false, 1);
        let schema = StructType::new_unchecked([field.clone()]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 1);

        let id_stats = stats.field("id").expect("id field should exist");
        let id_stats_struct = match id_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type"),
        };

        // Should have: value_count, lower_bound, upper_bound, exact_bounds
        assert_eq!(id_stats_struct.fields().count(), 4);
        assert!(id_stats_struct.field("value_count").is_some());
        assert!(id_stats_struct
            .field(crate::content_tree::NULL_COUNT_FIELD_NAME)
            .is_none()); // not nullable
        assert!(id_stats_struct.field("nan_value_count").is_none()); // not float/double
        assert!(id_stats_struct.field("avg_value_size").is_none()); // fixed-length
        assert!(id_stats_struct.field("max_value_size").is_none()); // fixed-length
        assert!(id_stats_struct.field("lower_bound").is_some());
        assert!(id_stats_struct.field("upper_bound").is_some());
        assert!(id_stats_struct.field("exact_bounds").is_some());

        // Check field IDs: base is 10_200 (field_id 1 -> 10_000 + 200*1)
        assert_stats_field_ids(id_stats_struct, 10_200, &field);

        // Verify the group-level field ID uses base_stats_id, not the original field ID
        assert_eq!(
            get_field_id(id_stats),
            Some(10_200),
            "Stats group field ID should be the base stats ID (10200), not the original field ID (1)"
        );
    }

    #[test]
    fn test_stats_schema_nullable_string() {
        // Nullable string field: should have 7 stats fields (includes null_value_count)
        let field = field_with_id("name", DataType::STRING, true, 2);
        let schema = StructType::new_unchecked([field.clone()]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let name_stats = stats.field("name").expect("name field should exist");
        let name_stats_struct = match name_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type"),
        };

        // Should have: value_count, null_value_count, avg_value_size, max_value_size, lower_bound, upper_bound, exact_bounds
        assert_eq!(name_stats_struct.fields().count(), 7);
        assert!(name_stats_struct
            .field(crate::content_tree::NULL_COUNT_FIELD_NAME)
            .is_some()); // nullable
        assert!(name_stats_struct.field("nan_value_count").is_none()); // not float/double

        // Check field IDs: base is 10_400
        assert_stats_field_ids(name_stats_struct, 10_400, &field);

        // Verify the group-level field ID uses base_stats_id
        assert_eq!(
            get_field_id(name_stats),
            Some(10_400),
            "Stats group field ID should be the base stats ID (10400), not the original field ID (2)"
        );
    }

    fn assert_stats_field_ids(stats_struct: &StructType, base_id: i32, field: &StructField) {
        assert_eq!(
            get_field_id(stats_struct.field("value_count").unwrap()),
            Some(base_id + STATS_OFFSET_VALUE_COUNT)
        );

        if field.is_nullable() {
            assert_eq!(
                get_field_id(
                    stats_struct
                        .field(crate::content_tree::NULL_COUNT_FIELD_NAME)
                        .unwrap()
                ),
                Some(base_id + STATS_OFFSET_NULL_VALUE_COUNT)
            );
        }

        if field.data_type.eq(&DataType::FLOAT) || field.data_type.eq(&DataType::DOUBLE) {
            assert_eq!(
                get_field_id(stats_struct.field("nan_value_count").unwrap()),
                Some(base_id + STATS_OFFSET_NAN_VALUE_COUNT)
            );
        }

        if field.data_type.eq(&DataType::STRING) || field.data_type.eq(&DataType::BINARY) {
            assert_eq!(
                get_field_id(stats_struct.field("avg_value_size").unwrap()),
                Some(base_id + STATS_OFFSET_AVG_VALUE_SIZE)
            );
            assert_eq!(
                get_field_id(stats_struct.field("max_value_size").unwrap()),
                Some(base_id + STATS_OFFSET_MAX_VALUE_SIZE)
            );
        }

        assert_eq!(
            get_field_id(stats_struct.field("lower_bound").unwrap()),
            Some(base_id + STATS_OFFSET_LOWER_BOUND)
        );
        assert_eq!(
            get_field_id(stats_struct.field("upper_bound").unwrap()),
            Some(base_id + STATS_OFFSET_UPPER_BOUND)
        );
        assert_eq!(
            get_field_id(stats_struct.field("exact_bounds").unwrap()),
            Some(base_id + STATS_OFFSET_EXACT_BOUNDS)
        );
    }

    #[test]
    fn test_stats_schema_nullable_double() {
        // Nullable double field: should have 6 stats fields
        // (includes null_value_count and nan_value_count; no size stats)
        let field = field_with_id("score", DataType::DOUBLE, true, 5);
        let schema = StructType::new_unchecked([field.clone()]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let score_stats = stats.field("score").expect("score field should exist");
        let score_stats_struct = match score_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type"),
        };

        assert_eq!(score_stats_struct.fields().count(), 6);
        assert!(score_stats_struct
            .field(crate::content_tree::NULL_COUNT_FIELD_NAME)
            .is_some()); // nullable
        assert!(score_stats_struct.field("nan_value_count").is_some()); // double
        assert!(score_stats_struct.field("avg_value_size").is_none()); // fixed-length
        assert!(score_stats_struct.field("max_value_size").is_none()); // fixed-length

        // Check field IDs: base is 11_000
        assert_stats_field_ids(score_stats_struct, 11_000, &field)
    }

    #[test]
    fn test_stats_schema_non_nullable_float() {
        // Non-nullable float field: should have 5 stats fields
        // (includes nan_value_count; no null_value_count; no size stats)
        let field = field_with_id("value", DataType::FLOAT, false, 100);
        let schema = StructType::new_unchecked([field.clone()]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let value_stats = stats.field("value").expect("value field should exist");
        let value_stats_struct = match value_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type"),
        };

        assert_eq!(value_stats_struct.fields().count(), 5);
        assert!(value_stats_struct
            .field(crate::content_tree::NULL_COUNT_FIELD_NAME)
            .is_none()); // not nullable
        assert!(value_stats_struct.field("nan_value_count").is_some()); // float
        assert!(value_stats_struct.field("avg_value_size").is_none()); // fixed-length
        assert!(value_stats_struct.field("max_value_size").is_none()); // fixed-length
        assert_stats_field_ids(value_stats_struct, 30_000, &field)
    }

    #[test]
    fn test_stats_schema_multiple_fields() {
        let schema = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, false, 0),
            field_with_id("name", DataType::STRING, true, 1),
            field_with_id("score", DataType::DOUBLE, true, 2),
        ]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 3);
        assert!(stats.field("id").is_some());
        assert!(stats.field("name").is_some());
        assert!(stats.field("score").is_some());
    }

    #[test]
    fn test_stats_schema_missing_field_id() {
        // Field without PARQUET:field_id should cause stats_schema to return an error
        let schema = StructType::new_unchecked([StructField::new("id", DataType::INTEGER, false)]);

        assert!(stats_schema(&schema).is_err());
    }

    #[test]
    fn test_stats_schema_bounds_preserve_type() {
        // Verify that lower_bound and upper_bound have the same type as the original field
        let field = field_with_id("amount", DataType::LONG, true, 42);
        let schema = StructType::new_unchecked([field.clone()]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let amount_stats = stats.field("amount").expect("amount field should exist");
        let amount_stats_struct = match amount_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type"),
        };

        let lower = amount_stats_struct.field("lower_bound").unwrap();
        let upper = amount_stats_struct.field("upper_bound").unwrap();
        assert_eq!(lower.data_type(), &DataType::LONG);
        assert_eq!(upper.data_type(), &DataType::LONG);
        assert_stats_field_ids(amount_stats_struct, 18_400, &field)
    }

    #[test]
    fn test_stats_schema_nested_struct() {
        // Test nested struct: { a: struct { b: int, c: double } }
        let field_b = field_with_id("b", DataType::INTEGER, false, 2);
        let field_c = field_with_id("c", DataType::DOUBLE, true, 3);
        let inner_struct = StructType::new_unchecked([field_b.clone(), field_c.clone()]);
        let schema = StructType::new_unchecked([field_with_id(
            "a",
            DataType::Struct(Box::new(inner_struct)),
            true,
            1,
        )]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 1);

        // The outer field 'a' should contain a struct
        let a_stats = stats.field("a").expect("a field should exist");
        let a_stats_struct = match a_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type for 'a'"),
        };

        // The inner struct should have 'b' and 'c' fields
        assert_eq!(a_stats_struct.fields().count(), 2);
        assert!(a_stats_struct.field("b").is_some());
        assert!(a_stats_struct.field("c").is_some());

        // Check 'b' stats (non-nullable int)
        let b_stats = a_stats_struct.field("b").unwrap();
        let b_stats_struct = match b_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type for 'b'"),
        };
        assert_eq!(b_stats_struct.fields().count(), 4); // no null_value_count, no nan_value_count, no size stats
        assert_stats_field_ids(b_stats_struct, 10_400, &field_b);

        // Check 'c' stats (nullable double)
        let c_stats = a_stats_struct.field("c").unwrap();
        let c_stats_struct = match c_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type for 'c'"),
        };
        assert_eq!(c_stats_struct.fields().count(), 6); // includes null_value_count and nan_value_count; no size stats
        assert_stats_field_ids(c_stats_struct, 10_600, &field_c);
    }

    #[test]
    fn test_stats_schema_array() {
        // TODO: This should produce statistics
        // Test array: { items: array<int> }
        let schema = StructType::new_unchecked([field_with_id(
            "items",
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))),
            true,
            1,
        )]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 1);

        let items_stats = stats.field("items").expect("items field should exist");
        let items_stats_struct = match items_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type for 'items'"),
        };

        // Lists are visited by DataType (not StructField), so primitive list elements have no
        // field-id context and therefore produce empty stats.
        assert_eq!(items_stats_struct.fields().count(), 0);
    }

    #[test]
    fn test_stats_schema_map() {
        // TODO: This should produce statistics
        // Test map: { mapping: map<string, int> }
        let schema = StructType::new_unchecked([field_with_id(
            "mapping",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::INTEGER,
                false,
            ))),
            true,
            1,
        )]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 1);

        let mapping_stats = stats.field("mapping").expect("mapping field should exist");
        let mapping_stats_struct = match mapping_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type for 'mapping'"),
        };

        // Maps are visited by DataType (not StructField), so primitive map key/value nodes have no
        // field-id context and therefore produce empty stats.
        assert_eq!(mapping_stats_struct.fields().count(), 0);
    }

    #[test]
    fn test_stats_schema_deeply_nested() {
        // Test deeply nested: { a: struct { b: struct { c: int } } }
        let innermost =
            StructType::new_unchecked([field_with_id("c", DataType::INTEGER, false, 3)]);
        let middle = StructType::new_unchecked([field_with_id(
            "b",
            DataType::Struct(Box::new(innermost)),
            true,
            2,
        )]);
        let schema = StructType::new_unchecked([field_with_id(
            "a",
            DataType::Struct(Box::new(middle)),
            true,
            1,
        )]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");

        // Navigate to a -> b -> c
        let a_stats = stats.field("a").unwrap();
        let a_struct = match a_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct"),
        };

        let b_stats = a_struct.field("b").unwrap();
        let b_struct = match b_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct"),
        };

        let c_stats = b_struct.field("c").unwrap();
        let c_struct = match c_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct"),
        };

        // 'c' is a non-nullable int, should have 4 stats fields
        assert_eq!(c_struct.fields().count(), 4);
        assert!(c_struct.field("value_count").is_some());
        assert!(c_struct.field("lower_bound").is_some());
    }

    /// Helper function to get a field's value from a StructData by field name
    fn get_struct_field<'a>(data: &'a StructData, name: &str) -> Option<&'a Scalar> {
        data.fields()
            .iter()
            .position(|f| f.name() == name)
            .map(|idx| &data.values()[idx])
    }

    /// Helper function to get a column's stats field value in AMT format
    /// AMT format: {col_name: {value_count, null_value_count?, lower_bound, upper_bound, exact_bounds}}
    fn get_column_stat<'a>(
        stats: &'a StructData,
        column: &str,
        stat_field: &str,
    ) -> Option<&'a Scalar> {
        if let Some(Scalar::Struct(col_stats)) = get_struct_field(stats, column) {
            get_struct_field(col_stats, stat_field)
        } else {
            None
        }
    }

    #[test]
    fn test_delta_json_stats_to_content_stats_basic() {
        // Create a table schema with field IDs
        let table_schema = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, false, 1),
            field_with_id("name", DataType::STRING, true, 2),
        ]);

        let stats_json = r#"{
            "numRecords": 100,
            "minValues": {"id": 1, "name": "alice"},
            "maxValues": {"id": 100, "name": "zoe"},
            "nullCount": {"id": 0, "name": 5}
        }"#;

        let content_stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)
            .expect("should convert stats")
            .expect("should have stats");

        // AMT format has one field per column: {id: {...}, name: {...}}
        assert_eq!(content_stats.fields().len(), 2);

        // Check id column stats (non-nullable LONG, so no null_value_count)
        assert_eq!(
            get_column_stat(&content_stats, "id", "value_count"),
            Some(&Scalar::Long(100))
        );
        assert_eq!(
            get_column_stat(&content_stats, "id", "lower_bound"),
            Some(&Scalar::Long(1))
        );
        assert_eq!(
            get_column_stat(&content_stats, "id", "upper_bound"),
            Some(&Scalar::Long(100))
        );
        assert_eq!(
            get_column_stat(&content_stats, "id", "exact_bounds"),
            Some(&Scalar::Boolean(true))
        );

        // Check name column stats (nullable STRING, so has null_value_count)
        assert_eq!(
            get_column_stat(&content_stats, "name", "value_count"),
            Some(&Scalar::Long(100))
        );
        assert_eq!(
            get_column_stat(
                &content_stats,
                "name",
                crate::content_tree::NULL_COUNT_FIELD_NAME
            ),
            Some(&Scalar::Long(5))
        );
        assert_eq!(
            get_column_stat(&content_stats, "name", "lower_bound"),
            Some(&Scalar::String("alice".to_string()))
        );
        assert_eq!(
            get_column_stat(&content_stats, "name", "upper_bound"),
            Some(&Scalar::String("zoe".to_string()))
        );
    }

    #[test]
    fn test_delta_json_stats_to_content_stats_none() {
        let table_schema =
            StructType::new_unchecked([field_with_id("id", DataType::LONG, false, 1)]);

        // None input should return None
        let result =
            delta_json_stats_to_content_stats(None, &table_schema).expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn test_delta_json_stats_to_content_stats_invalid_json() {
        let table_schema =
            StructType::new_unchecked([field_with_id("id", DataType::LONG, false, 1)]);

        // Invalid JSON should return None (graceful handling)
        let result = delta_json_stats_to_content_stats(Some("not valid json"), &table_schema)
            .expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn test_delta_json_stats_to_content_stats_partial_stats() {
        // Test with partial stats (only numRecords, no min/max/null)
        let table_schema =
            StructType::new_unchecked([field_with_id("id", DataType::LONG, true, 1)]);

        let stats_json = r#"{"numRecords": 50}"#;

        let content_stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)
            .expect("should convert stats")
            .expect("should have stats");

        // AMT format has one field per column
        assert_eq!(content_stats.fields().len(), 1);

        // value_count should be populated from numRecords
        assert_eq!(
            get_column_stat(&content_stats, "id", "value_count"),
            Some(&Scalar::Long(50))
        );

        // exact_bounds should default to true
        assert_eq!(
            get_column_stat(&content_stats, "id", "exact_bounds"),
            Some(&Scalar::Boolean(true))
        );
    }

    #[test]
    fn test_delta_json_stats_to_content_stats_numeric_types() {
        // Test various numeric types
        let table_schema = StructType::new_unchecked([
            field_with_id("int_col", DataType::INTEGER, false, 1),
            field_with_id("short_col", DataType::SHORT, false, 2),
            field_with_id("byte_col", DataType::BYTE, false, 3),
            field_with_id("double_col", DataType::DOUBLE, true, 4),
            field_with_id("float_col", DataType::FLOAT, true, 5),
        ]);

        let stats_json = r#"{
            "numRecords": 10,
            "minValues": {"int_col": -100, "short_col": -10, "byte_col": -1, "double_col": 0.5, "float_col": 1.5},
            "maxValues": {"int_col": 100, "short_col": 10, "byte_col": 1, "double_col": 99.9, "float_col": 9.9}
        }"#;

        let content_stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)
            .expect("should convert stats")
            .expect("should have stats");

        // AMT format: one field per column
        assert_eq!(content_stats.fields().len(), 5);

        // Verify int_col lower/upper bounds
        assert_eq!(
            get_column_stat(&content_stats, "int_col", "lower_bound"),
            Some(&Scalar::Integer(-100))
        );
        assert_eq!(
            get_column_stat(&content_stats, "int_col", "upper_bound"),
            Some(&Scalar::Integer(100))
        );

        // Verify double_col lower/upper bounds
        assert_eq!(
            get_column_stat(&content_stats, "double_col", "lower_bound"),
            Some(&Scalar::Double(0.5))
        );
        assert_eq!(
            get_column_stat(&content_stats, "double_col", "upper_bound"),
            Some(&Scalar::Double(99.9))
        );
    }

    #[test]
    fn test_delta_json_stats_tight_bounds_true() {
        // Test with tightBounds: true (explicit)
        let table_schema =
            StructType::new_unchecked([field_with_id("value", DataType::LONG, false, 1)]);

        let stats_json = r#"{
            "numRecords": 10,
            "minValues": {"value": 0},
            "maxValues": {"value": 9},
            "nullCount": {"value": 0},
            "tightBounds": true
        }"#;

        let content_stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)
            .expect("should convert stats")
            .expect("should have stats");

        // In AMT format, exact_bounds is per-column
        assert_eq!(
            get_column_stat(&content_stats, "value", "exact_bounds"),
            Some(&Scalar::Boolean(true)),
            "exact_bounds should be true"
        );
    }

    #[test]
    fn test_delta_json_stats_tight_bounds_false() {
        // Test with tightBounds: false (e.g., file has deletion vectors)
        let table_schema =
            StructType::new_unchecked([field_with_id("value", DataType::LONG, false, 1)]);

        let stats_json = r#"{
            "numRecords": 10,
            "minValues": {"value": 0},
            "maxValues": {"value": 9},
            "nullCount": {"value": 0},
            "tightBounds": false
        }"#;

        let content_stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)
            .expect("should convert stats")
            .expect("should have stats");

        // In AMT format, exact_bounds is per-column
        assert_eq!(
            get_column_stat(&content_stats, "value", "exact_bounds"),
            Some(&Scalar::Boolean(false)),
            "exact_bounds should be false"
        );
    }

    #[test]
    fn test_delta_json_stats_tight_bounds_default() {
        // Test without tightBounds field (should default to true for backwards compatibility)
        let table_schema =
            StructType::new_unchecked([field_with_id("value", DataType::LONG, false, 1)]);

        let stats_json = r#"{
            "numRecords": 10,
            "minValues": {"value": 0},
            "maxValues": {"value": 9},
            "nullCount": {"value": 0}
        }"#;

        let content_stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)
            .expect("should convert stats")
            .expect("should have stats");

        // In AMT format, exact_bounds defaults to true when tightBounds is absent
        assert_eq!(
            get_column_stat(&content_stats, "value", "exact_bounds"),
            Some(&Scalar::Boolean(true)),
            "exact_bounds should default to true when tightBounds is absent"
        );
    }

    #[test]
    fn test_delta_json_stats_timestamp_parsing() {
        // Test timestamp parsing with RFC3339 format (as used in Delta stats)
        let table_schema =
            StructType::new_unchecked([field_with_id("ts", DataType::TIMESTAMP, true, 1)]);

        // Format from Delta Protocol: "2023-05-31T18:58:33.633Z"
        let stats_json = r#"{
            "numRecords": 5,
            "minValues": {"ts": "2023-05-31T18:58:33.633Z"},
            "maxValues": {"ts": "2023-05-31T18:58:33.633Z"},
            "nullCount": {"ts": 0}
        }"#;

        let content_stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)
            .expect("should convert stats")
            .expect("should have stats");

        // Get timestamp from lower_bound in AMT format
        let min_ts = get_column_stat(&content_stats, "ts", "lower_bound")
            .expect("should have lower_bound for ts");

        // Verify it's a valid timestamp
        match min_ts {
            Scalar::Timestamp(micros) => {
                // 2023-05-31T18:58:33.633Z in microseconds since epoch
                // Should be approximately 1685559513633000 microseconds
                assert!(*micros > 0, "timestamp should be positive");
                assert!(
                    *micros > 1685559513000000 && *micros < 1685559514000000,
                    "timestamp should be around 2023-05-31T18:58:33.633Z, got {}",
                    micros
                );
            }
            other => panic!("Expected Timestamp scalar, got {:?}", other),
        }
    }

    #[test]
    fn test_delta_json_stats_date_parsing() {
        // Test date parsing with YYYY-MM-DD format
        let table_schema =
            StructType::new_unchecked([field_with_id("date_col", DataType::DATE, true, 1)]);

        let stats_json = r#"{
            "numRecords": 10,
            "minValues": {"date_col": "2023-01-15"},
            "maxValues": {"date_col": "2023-12-31"},
            "nullCount": {"date_col": 0}
        }"#;

        let content_stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)
            .expect("should convert stats")
            .expect("should have stats");

        // Get date from lower_bound and upper_bound in AMT format
        let min_date = get_column_stat(&content_stats, "date_col", "lower_bound")
            .expect("should have lower_bound for date_col");
        let max_date = get_column_stat(&content_stats, "date_col", "upper_bound")
            .expect("should have upper_bound for date_col");

        // 2023-01-15 is 19372 days since 1970-01-01
        // 2023-12-31 is 19722 days since 1970-01-01
        match min_date {
            Scalar::Date(days) => {
                assert_eq!(*days, 19372, "2023-01-15 should be 19372 days since epoch");
            }
            other => panic!("Expected Date scalar for lower_bound, got {:?}", other),
        }

        match max_date {
            Scalar::Date(days) => {
                assert_eq!(*days, 19722, "2023-12-31 should be 19722 days since epoch");
            }
            other => panic!("Expected Date scalar for upper_bound, got {:?}", other),
        }
    }

    #[test]
    fn test_aggregate_content_stats_basic() {
        // Create a table schema with field IDs
        let table_schema = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, false, 1),
            field_with_id("name", DataType::STRING, true, 2),
        ]);

        // Create content_stats for file 1: id=[1, 50], name=["alice", "mike"]
        let stats1_json = r#"{
            "numRecords": 100,
            "minValues": {"id": 1, "name": "alice"},
            "maxValues": {"id": 50, "name": "mike"},
            "nullCount": {"id": 0, "name": 5}
        }"#;
        let stats1 = delta_json_stats_to_content_stats(Some(stats1_json), &table_schema)
            .expect("should convert")
            .expect("should have stats");

        // Create content_stats for file 2: id=[40, 100], name=["bob", "zoe"]
        let stats2_json = r#"{
            "numRecords": 150,
            "minValues": {"id": 40, "name": "bob"},
            "maxValues": {"id": 100, "name": "zoe"},
            "nullCount": {"id": 0, "name": 10}
        }"#;
        let stats2 = delta_json_stats_to_content_stats(Some(stats2_json), &table_schema)
            .expect("should convert")
            .expect("should have stats");

        // Aggregate
        let aggregated = aggregate_content_stats([Some(&stats1), Some(&stats2)].into_iter())
            .expect("should aggregate");

        // AMT format aggregation:
        // - id.value_count: 100 + 150 = 250
        // - id.lower_bound: min(1, 40) = 1
        // - id.upper_bound: max(50, 100) = 100
        // - name.value_count: 100 + 150 = 250
        // - name.null_value_count: 5 + 10 = 15
        // - name.lower_bound: min("alice", "bob") = "alice"
        // - name.upper_bound: max("mike", "zoe") = "zoe"

        assert_eq!(
            get_column_stat(&aggregated, "id", "value_count"),
            Some(&Scalar::Long(250))
        );
        assert_eq!(
            get_column_stat(&aggregated, "id", "lower_bound"),
            Some(&Scalar::Long(1))
        );
        assert_eq!(
            get_column_stat(&aggregated, "id", "upper_bound"),
            Some(&Scalar::Long(100))
        );

        assert_eq!(
            get_column_stat(&aggregated, "name", "value_count"),
            Some(&Scalar::Long(250))
        );
        assert_eq!(
            get_column_stat(
                &aggregated,
                "name",
                crate::content_tree::NULL_COUNT_FIELD_NAME
            ),
            Some(&Scalar::Long(15))
        );
        assert_eq!(
            get_column_stat(&aggregated, "name", "lower_bound"),
            Some(&Scalar::String("alice".to_string()))
        );
        assert_eq!(
            get_column_stat(&aggregated, "name", "upper_bound"),
            Some(&Scalar::String("zoe".to_string()))
        );
    }

    #[test]
    fn test_aggregate_content_stats_with_none() {
        // Test that None entries are skipped
        let table_schema =
            StructType::new_unchecked([field_with_id("id", DataType::LONG, false, 1)]);

        let stats_json = r#"{
            "numRecords": 100,
            "minValues": {"id": 1},
            "maxValues": {"id": 50}
        }"#;
        let stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)
            .expect("should convert")
            .expect("should have stats");

        // Aggregate with None entries mixed in
        let aggregated = aggregate_content_stats([None, Some(&stats), None].into_iter())
            .expect("should aggregate");

        // Should have the same values as the single input
        assert_eq!(
            get_column_stat(&aggregated, "id", "value_count"),
            Some(&Scalar::Long(100))
        );
        assert_eq!(
            get_column_stat(&aggregated, "id", "lower_bound"),
            Some(&Scalar::Long(1))
        );
        assert_eq!(
            get_column_stat(&aggregated, "id", "upper_bound"),
            Some(&Scalar::Long(50))
        );
    }

    #[test]
    fn test_aggregate_content_stats_all_none() {
        // Test that all None returns None
        let result = aggregate_content_stats([None, None, None].into_iter());
        assert!(result.is_none());
    }

    #[test]
    fn test_aggregate_content_stats_empty() {
        // Test empty iterator returns None
        let result = aggregate_content_stats(std::iter::empty());
        assert!(result.is_none());
    }

    #[test]
    fn test_aggregate_content_stats_exact_bounds() {
        // Test that exact_bounds is AND'ed across all entries
        let table_schema =
            StructType::new_unchecked([field_with_id("value", DataType::LONG, false, 1)]);

        // File 1 with tightBounds: true
        let stats1_json = r#"{
            "numRecords": 100,
            "minValues": {"value": 1},
            "maxValues": {"value": 50},
            "tightBounds": true
        }"#;
        let stats1 = delta_json_stats_to_content_stats(Some(stats1_json), &table_schema)
            .expect("should convert")
            .expect("should have stats");

        // File 2 with tightBounds: false (e.g., has deletion vector)
        let stats2_json = r#"{
            "numRecords": 100,
            "minValues": {"value": 40},
            "maxValues": {"value": 100},
            "tightBounds": false
        }"#;
        let stats2 = delta_json_stats_to_content_stats(Some(stats2_json), &table_schema)
            .expect("should convert")
            .expect("should have stats");

        // Aggregate - exact_bounds should be false because one input is false
        let aggregated = aggregate_content_stats([Some(&stats1), Some(&stats2)].into_iter())
            .expect("should aggregate");

        // In AMT format, exact_bounds is per-column
        assert_eq!(
            get_column_stat(&aggregated, "value", "exact_bounds"),
            Some(&Scalar::Boolean(false)),
            "exact_bounds should be false when any input is false"
        );
    }

    #[test]
    fn test_aggregate_content_stats_all_exact_bounds_true() {
        // Test that exact_bounds is true when all inputs are true
        let table_schema =
            StructType::new_unchecked([field_with_id("value", DataType::LONG, false, 1)]);

        let stats1_json = r#"{
            "numRecords": 100,
            "minValues": {"value": 1},
            "maxValues": {"value": 50},
            "tightBounds": true
        }"#;
        let stats1 = delta_json_stats_to_content_stats(Some(stats1_json), &table_schema)
            .expect("should convert")
            .expect("should have stats");

        let stats2_json = r#"{
            "numRecords": 100,
            "minValues": {"value": 40},
            "maxValues": {"value": 100},
            "tightBounds": true
        }"#;
        let stats2 = delta_json_stats_to_content_stats(Some(stats2_json), &table_schema)
            .expect("should convert")
            .expect("should have stats");

        let aggregated = aggregate_content_stats([Some(&stats1), Some(&stats2)].into_iter())
            .expect("should aggregate");

        // In AMT format, exact_bounds is per-column
        assert_eq!(
            get_column_stat(&aggregated, "value", "exact_bounds"),
            Some(&Scalar::Boolean(true)),
            "exact_bounds should be true when all inputs are true"
        );
    }

    #[test]
    fn test_aggregate_content_stats_nested_struct() {
        // Test aggregation with nested struct columns using AMT format
        let inner_struct = StructType::new_unchecked([
            field_with_id("nested_id", DataType::LONG, false, 2),
            field_with_id("nested_name", DataType::STRING, true, 3),
        ]);
        let table_schema = StructType::new_unchecked([field_with_id(
            "outer",
            DataType::Struct(Box::new(inner_struct)),
            true,
            1,
        )]);

        // Note: Delta JSON stats use dot-notation for nested fields
        let stats1_json = r#"{
            "numRecords": 100,
            "minValues": {"outer.nested_id": 1, "outer.nested_name": "alice"},
            "maxValues": {"outer.nested_id": 50, "outer.nested_name": "mike"},
            "nullCount": {"outer.nested_name": 5}
        }"#;
        let stats1 = delta_json_stats_to_content_stats(Some(stats1_json), &table_schema)
            .expect("should convert")
            .expect("should have stats");

        let stats2_json = r#"{
            "numRecords": 150,
            "minValues": {"outer.nested_id": 40, "outer.nested_name": "bob"},
            "maxValues": {"outer.nested_id": 100, "outer.nested_name": "zoe"},
            "nullCount": {"outer.nested_name": 10}
        }"#;
        let stats2 = delta_json_stats_to_content_stats(Some(stats2_json), &table_schema)
            .expect("should convert")
            .expect("should have stats");

        let aggregated = aggregate_content_stats([Some(&stats1), Some(&stats2)].into_iter())
            .expect("should aggregate");

        // AMT format aggregation for nested structs:
        // outer.nested_id.value_count: 100 + 150 = 250
        // outer.nested_id.lower_bound: min(1, 40) = 1
        // outer.nested_id.upper_bound: max(50, 100) = 100

        // In AMT format, nested structs preserve the hierarchy
        // {outer: {nested_id: {value_count, lower_bound, ...}, nested_name: {...}}}
        if let Some(Scalar::Struct(outer_stats)) = get_struct_field(&aggregated, "outer") {
            // Check nested_id stats
            if let Some(Scalar::Struct(nested_id_stats)) =
                get_struct_field(outer_stats, "nested_id")
            {
                assert_eq!(
                    get_struct_field(nested_id_stats, "value_count"),
                    Some(&Scalar::Long(250))
                );
                assert_eq!(
                    get_struct_field(nested_id_stats, "lower_bound"),
                    Some(&Scalar::Long(1))
                );
                assert_eq!(
                    get_struct_field(nested_id_stats, "upper_bound"),
                    Some(&Scalar::Long(100))
                );
            }

            // Check nested_name stats
            if let Some(Scalar::Struct(nested_name_stats)) =
                get_struct_field(outer_stats, "nested_name")
            {
                assert_eq!(
                    get_struct_field(
                        nested_name_stats,
                        crate::content_tree::NULL_COUNT_FIELD_NAME
                    ),
                    Some(&Scalar::Long(15))
                );
                assert_eq!(
                    get_struct_field(nested_name_stats, "lower_bound"),
                    Some(&Scalar::String("alice".to_string()))
                );
                assert_eq!(
                    get_struct_field(nested_name_stats, "upper_bound"),
                    Some(&Scalar::String("zoe".to_string()))
                );
            }
        }
    }
}
