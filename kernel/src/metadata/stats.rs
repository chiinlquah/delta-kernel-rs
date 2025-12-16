//! Stats field ID calculation utilities for Adaptive Metadata Tree (AMT).
//!
//! This module provides functions to compute stats field IDs for parent struct fields,
//! which are used in the AMT format for storing per-column statistics.

use crate::schema::visitor::{visit_struct, SchemaVisitor};
use crate::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, PrimitiveType, StructField,
    StructType,
};
use crate::DeltaResult;

/// Number of stats slots reserved per column.
const NUM_STATS_PER_COLUMN: i32 = 200;

/// Number of reserved field IDs at the top of the i32 range.
const RESERVED_FIELD_IDS: i32 = 200;

/// Starting field ID for the data space (regular column stats).
const DATA_SPACE_FIELD_ID_START: i32 = 10_000;

/// Starting field ID for the metadata space (reserved field stats).
const METADATA_SPACE_FIELD_ID_START: i32 = 2_147_000_000;

/// Field ID where reserved field IDs begin.
const RESERVED_FIELD_IDS_START: i32 = i32::MAX - RESERVED_FIELD_IDS;

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
#[allow(dead_code)]
pub(crate) fn field_id_to_statistics_base(field_id: i32) -> Option<i32> {
    if field_id < 0 {
        // Short circuit on negative field-IDs
        return None;
    }

    let (id_space_start, id) = if field_id >= RESERVED_FIELD_IDS_START {
        // This is a reserved field ID, which uses a different calculation
        let id = RESERVED_FIELD_IDS - (i32::MAX - field_id);
        (METADATA_SPACE_FIELD_ID_START, id)
    } else {
        (DATA_SPACE_FIELD_ID_START, field_id)
    };

    // Calculate final_id, checking for overflow
    let stats_offset = NUM_STATS_PER_COLUMN.checked_mul(id)?;
    let final_id = id_space_start.checked_add(stats_offset)?;

    // Check for overlap with other ID ranges:
    // Data space IDs should not overlap into metadata space
    if field_id < RESERVED_FIELD_IDS_START && final_id >= METADATA_SPACE_FIELD_ID_START {
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
#[allow(dead_code)]
pub(crate) fn statistics_base_to_field_id(stats_field_id: i32) -> Option<i32> {
    // Invalid stats field ID: negative or not a multiple of NUM_STATS_PER_COLUMN
    if stats_field_id < 0 || stats_field_id % NUM_STATS_PER_COLUMN != 0 {
        return None;
    }

    let final_id = if stats_field_id < METADATA_SPACE_FIELD_ID_START {
        // Data space: reverse the calculation
        // stats_field_id = DATA_SPACE_FIELD_ID_START + NUM_STATS_PER_COLUMN * field_id
        // => field_id = (stats_field_id - DATA_SPACE_FIELD_ID_START) / NUM_STATS_PER_COLUMN
        (stats_field_id - DATA_SPACE_FIELD_ID_START) / NUM_STATS_PER_COLUMN
    } else {
        // Metadata space (reserved field IDs): reverse the calculation
        // stats_field_id = METADATA_SPACE_FIELD_ID_START + NUM_STATS_PER_COLUMN * id
        // where id = RESERVED_FIELD_IDS - (i32::MAX - field_id)
        // => id = (stats_field_id - METADATA_SPACE_FIELD_ID_START) / NUM_STATS_PER_COLUMN
        // => field_id = i32::MAX - (RESERVED_FIELD_IDS - id) = RESERVED_FIELD_IDS_START + id
        let id = (stats_field_id - METADATA_SPACE_FIELD_ID_START) / NUM_STATS_PER_COLUMN;
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
fn field_with_id(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> StructField {
    StructField::new(name, data_type, nullable).with_metadata([(
        ColumnMetadataKey::ParquetFieldId.as_ref(),
        MetadataValue::Number(field_id as i64),
    )])
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
                "Field '{}' is missing parquet.field.id metadata",
                field.name()
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

        Ok(vec![StructField::new(
            field.name(),
            DataType::Struct(Box::new(stats_struct)),
            true,
        )])
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
/// - offset 4: `avg_value_size` (long) - only for variable-length types (e.g. string/binary)
/// - offset 5: `max_value_size` (long) - only for variable-length types (e.g. string/binary)
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
            "null_value_count",
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
            DataType::LONG,
            true,
            base_field_id + STATS_OFFSET_AVG_VALUE_SIZE,
        ));

        fields.push(field_with_id(
            "max_value_size",
            DataType::LONG,
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
/// - `avg_value_size` (long): average size of values (only for variable-length types)
/// - `max_value_size` (long): maximum size of values (only for variable-length types)
/// - `lower_bound` (same type as field): minimum value
/// - `upper_bound` (same type as field): maximum value
/// - `exact_bounds` (boolean): whether bounds are exact
///
/// ## Field IDs
///
/// Field IDs in the stats schema are computed using [`field_id_to_statistics_base`]
/// based on the original field's `parquet.field.id` metadata.
///
/// # Arguments
///
/// * `table_struct` - The struct type to generate stats schema for
///
/// # Returns
///
/// Returns `Ok(StructType)` containing the stats schema, or an error if:
/// - Any field is missing the `parquet.field.id` metadata
/// - Field ID computation fails (e.g., overflow)
#[allow(dead_code)]
pub(crate) fn stats_schema(table_struct: &StructType) -> DeltaResult<StructType> {
    let mut visitor = StatsSchemaVisitor;
    let fields = visit_struct(table_struct, &mut visitor)?;
    Ok(StructType::new_unchecked(fields))
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
            (2_147_000_000, Some(2_147_483_447)),
            (2_147_000_200, Some(2_147_483_448)),
            (2_147_018_800, Some(2_147_483_541)),
            (2_147_039_600, Some(2_147_483_645)),
            (2_147_039_800, Some(2_147_483_646)),
            // Invalid cases
            (-1, None),     // negative
            (10_001, None), // not a multiple of 200
            (0, None),      // below data space start (would give negative field_id)
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
        let schema = StructType::new_unchecked([field_with_id("id", DataType::INTEGER, false, 1)]);

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
        assert!(id_stats_struct.field("null_value_count").is_none()); // not nullable
        assert!(id_stats_struct.field("nan_value_count").is_none()); // not float/double
        assert!(id_stats_struct.field("avg_value_size").is_none()); // fixed-length
        assert!(id_stats_struct.field("max_value_size").is_none()); // fixed-length
        assert!(id_stats_struct.field("lower_bound").is_some());
        assert!(id_stats_struct.field("upper_bound").is_some());
        assert!(id_stats_struct.field("exact_bounds").is_some());

        // Check field IDs: base is 10_200 (field_id 1 -> 10_000 + 200*1)
        let base_id = 10_200;
        assert_eq!(
            get_field_id(id_stats_struct.field("value_count").unwrap()),
            Some(base_id + 1)
        );
        assert_eq!(
            get_field_id(id_stats_struct.field("lower_bound").unwrap()),
            Some(base_id + 6)
        );
        assert_eq!(
            get_field_id(id_stats_struct.field("upper_bound").unwrap()),
            Some(base_id + 7)
        );
    }

    #[test]
    fn test_stats_schema_nullable_string() {
        // Nullable string field: should have 7 stats fields (includes null_value_count)
        let schema = StructType::new_unchecked([field_with_id("name", DataType::STRING, true, 2)]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let name_stats = stats.field("name").expect("name field should exist");
        let name_stats_struct = match name_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type"),
        };

        // Should have: value_count, null_value_count, avg_value_size, max_value_size, lower_bound, upper_bound, exact_bounds
        assert_eq!(name_stats_struct.fields().count(), 7);
        assert!(name_stats_struct.field("null_value_count").is_some()); // nullable
        assert!(name_stats_struct.field("nan_value_count").is_none()); // not float/double

        // Check field IDs: base is 10_400 (field_id 2 -> 10_000 + 200*2)
        let base_id = 10_400;
        assert_eq!(
            get_field_id(name_stats_struct.field("null_value_count").unwrap()),
            Some(base_id + 2)
        );
    }

    #[test]
    fn test_stats_schema_nullable_double() {
        // Nullable double field: should have 6 stats fields
        // (includes null_value_count and nan_value_count; no size stats)
        let schema = StructType::new_unchecked([field_with_id("score", DataType::DOUBLE, true, 5)]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let score_stats = stats.field("score").expect("score field should exist");
        let score_stats_struct = match score_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type"),
        };

        assert_eq!(score_stats_struct.fields().count(), 6);
        assert!(score_stats_struct.field("null_value_count").is_some()); // nullable
        assert!(score_stats_struct.field("nan_value_count").is_some()); // double
        assert!(score_stats_struct.field("avg_value_size").is_none()); // fixed-length
        assert!(score_stats_struct.field("max_value_size").is_none()); // fixed-length

        // Check field IDs: base is 11_000 (field_id 5 -> 10_000 + 200*5)
        let base_id = 11_000;
        assert_eq!(
            get_field_id(score_stats_struct.field("nan_value_count").unwrap()),
            Some(base_id + 3)
        );
    }

    #[test]
    fn test_stats_schema_non_nullable_float() {
        // Non-nullable float field: should have 5 stats fields
        // (includes nan_value_count; no null_value_count; no size stats)
        let schema =
            StructType::new_unchecked([field_with_id("value", DataType::FLOAT, false, 100)]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let value_stats = stats.field("value").expect("value field should exist");
        let value_stats_struct = match value_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type"),
        };

        assert_eq!(value_stats_struct.fields().count(), 5);
        assert!(value_stats_struct.field("null_value_count").is_none()); // not nullable
        assert!(value_stats_struct.field("nan_value_count").is_some()); // float
        assert!(value_stats_struct.field("avg_value_size").is_none()); // fixed-length
        assert!(value_stats_struct.field("max_value_size").is_none()); // fixed-length
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
        // Field without parquet.field.id should cause stats_schema to return an error
        let schema = StructType::new_unchecked([StructField::new("id", DataType::INTEGER, false)]);

        assert!(stats_schema(&schema).is_err());
    }

    #[test]
    fn test_stats_schema_bounds_preserve_type() {
        // Verify that lower_bound and upper_bound have the same type as the original field
        let schema = StructType::new_unchecked([field_with_id("amount", DataType::LONG, true, 42)]);

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
    }

    #[test]
    fn test_stats_schema_nested_struct() {
        // Test nested struct: { a: struct { b: int, c: double } }
        let inner_struct = StructType::new_unchecked([
            field_with_id("b", DataType::INTEGER, false, 2),
            field_with_id("c", DataType::DOUBLE, true, 3),
        ]);
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

        // Check 'c' stats (nullable double)
        let c_stats = a_stats_struct.field("c").unwrap();
        let c_stats_struct = match c_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected struct type for 'c'"),
        };
        assert_eq!(c_stats_struct.fields().count(), 6); // includes null_value_count and nan_value_count; no size stats
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
}
