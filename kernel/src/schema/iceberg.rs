//! Conversion from Delta schema types to Iceberg schema types.
//!
//! This module provides the [`delta_schema_to_iceberg`] function that converts a Delta
//! [`StructType`] to an Iceberg [`iceberg::spec::Schema`].
//!
//! The conversion requires column mapping in `id` mode -- every [`StructField`] must have a
//! `delta.columnMapping.id` metadata entry.

use iceberg::spec as iceberg_spec;

use super::{ColumnMetadataKey, DataType, MetadataValue, PrimitiveType};
use super::{StructField, StructType};
use crate::{DeltaResult, Error};

// ---------------------------------------------------------------------------
// Type conversion
// ---------------------------------------------------------------------------

/// Converts a Delta [`PrimitiveType`] to an Iceberg [`iceberg_spec::PrimitiveType`].
fn convert_primitive(primitive: &PrimitiveType) -> DeltaResult<iceberg_spec::PrimitiveType> {
    match primitive {
        PrimitiveType::String => Ok(iceberg_spec::PrimitiveType::String),
        PrimitiveType::Long => Ok(iceberg_spec::PrimitiveType::Long),
        PrimitiveType::Integer => Ok(iceberg_spec::PrimitiveType::Int),
        PrimitiveType::Short => Ok(iceberg_spec::PrimitiveType::Int),
        PrimitiveType::Byte => Ok(iceberg_spec::PrimitiveType::Int),
        PrimitiveType::Float => Ok(iceberg_spec::PrimitiveType::Float),
        PrimitiveType::Double => Ok(iceberg_spec::PrimitiveType::Double),
        PrimitiveType::Boolean => Ok(iceberg_spec::PrimitiveType::Boolean),
        PrimitiveType::Binary => Ok(iceberg_spec::PrimitiveType::Binary),
        PrimitiveType::Date => Ok(iceberg_spec::PrimitiveType::Date),
        PrimitiveType::Timestamp => Ok(iceberg_spec::PrimitiveType::Timestamptz),
        PrimitiveType::TimestampNtz => Ok(iceberg_spec::PrimitiveType::Timestamp),
        PrimitiveType::Decimal(d) => Ok(iceberg_spec::PrimitiveType::Decimal {
            precision: d.precision() as u32,
            scale: d.scale() as u32,
        }),
    }
}

/// Converts a Delta [`DataType`] to an Iceberg [`iceberg_spec::Type`].
fn convert_type(data_type: &DataType) -> DeltaResult<iceberg_spec::Type> {
    match data_type {
        DataType::Primitive(p) => {
            Ok(iceberg_spec::Type::Primitive(convert_primitive(p)?))
        }
        DataType::Struct(s) => {
            let iceberg_struct = convert_struct(s)?;
            Ok(iceberg_spec::Type::Struct(iceberg_struct))
        }
        DataType::Array(_) => Err(Error::generic(
            "Array type is not yet supported for Iceberg schema conversion",
        )),
        DataType::Map(_) => Err(Error::generic(
            "Map type is not yet supported for Iceberg schema conversion",
        )),
        DataType::Variant(_) => Err(Error::generic(
            "Variant type is not yet supported for Iceberg schema conversion",
        )),
    }
}

// ---------------------------------------------------------------------------
// Field and schema conversion
// ---------------------------------------------------------------------------

/// Extracts the column mapping field ID from a [`StructField`].
fn get_field_id(field: &StructField) -> DeltaResult<i32> {
    match field.get_config_value(&ColumnMetadataKey::ColumnMappingId) {
        Some(MetadataValue::Number(id)) => Ok(*id as i32),
        _ => Err(Error::generic(format!(
            "Field '{}': missing or invalid {} metadata required for Iceberg schema conversion",
            field.name,
            ColumnMetadataKey::ColumnMappingId.as_ref()
        ))),
    }
}

/// Converts a Delta [`StructField`] to an Iceberg [`iceberg_spec::NestedField`].
fn convert_field(field: &StructField) -> DeltaResult<iceberg_spec::NestedField> {
    let id = get_field_id(field)?;
    let field_type = convert_type(&field.data_type)?;

    if field.nullable {
        Ok(iceberg_spec::NestedField::optional(
            id,
            &field.name,
            field_type,
        ))
    } else {
        Ok(iceberg_spec::NestedField::required(
            id,
            &field.name,
            field_type,
        ))
    }
}

/// Converts a Delta [`StructType`] to an Iceberg [`iceberg_spec::StructType`].
fn convert_struct(struct_type: &StructType) -> DeltaResult<iceberg_spec::StructType> {
    let fields: Vec<iceberg_spec::NestedFieldRef> = struct_type
        .fields()
        .map(|f| Ok(convert_field(f)?.into()))
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(iceberg_spec::StructType::new(fields))
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Converts a Delta [`StructType`] (table schema) to an Iceberg [`iceberg_spec::Schema`].
///
/// The schema must have column mapping metadata in `id` mode -- every field must have a
/// `delta.columnMapping.id` entry.
///
/// # Parameters
///
/// - `schema`: The Delta table schema to convert.
/// - `schema_id`: The Iceberg schema ID (typically 0 for the current schema).
/// - `identifier_field_ids`: Field IDs that form the identifier (primary key) of the table.
///   Pass an empty vector if the table has no identifier fields.
///
/// # Errors
///
/// Returns an error if:
/// - Any field is missing `delta.columnMapping.id` metadata.
/// - An array, map, or variant type is encountered (not yet supported).
pub(crate) fn delta_schema_to_iceberg(
    schema: &StructType,
    schema_id: i32,
    identifier_field_ids: Vec<i32>,
) -> DeltaResult<iceberg_spec::Schema> {
    let fields: Vec<iceberg_spec::NestedFieldRef> = schema
        .fields()
        .map(|f| Ok(convert_field(f)?.into()))
        .collect::<DeltaResult<Vec<_>>>()?;

    iceberg_spec::Schema::builder()
        .with_schema_id(schema_id)
        .with_fields(fields)
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| Error::generic(format!("Failed to build Iceberg schema: {}", e)))
}

// ===========================================================================
// Iceberg -> Delta conversion
// ===========================================================================

/// Converts an Iceberg [`iceberg_spec::PrimitiveType`] to a Delta [`PrimitiveType`].
///
/// Note: Iceberg `Int` maps to Delta `Integer` (not `Short` or `Byte`, since Iceberg has no
/// distinction). Iceberg `Time`, `Uuid`, `Fixed`, `TimestampNs`, and `TimestamptzNs` have no
/// Delta equivalents and return an error.
fn iceberg_primitive_to_delta(
    primitive: &iceberg_spec::PrimitiveType,
) -> DeltaResult<PrimitiveType> {
    match primitive {
        iceberg_spec::PrimitiveType::Boolean => Ok(PrimitiveType::Boolean),
        iceberg_spec::PrimitiveType::Int => Ok(PrimitiveType::Integer),
        iceberg_spec::PrimitiveType::Long => Ok(PrimitiveType::Long),
        iceberg_spec::PrimitiveType::Float => Ok(PrimitiveType::Float),
        iceberg_spec::PrimitiveType::Double => Ok(PrimitiveType::Double),
        iceberg_spec::PrimitiveType::Decimal { precision, scale } => {
            let dt = crate::schema::DecimalType::try_new(*precision as u8, *scale as u8)?;
            Ok(PrimitiveType::Decimal(dt))
        }
        iceberg_spec::PrimitiveType::Date => Ok(PrimitiveType::Date),
        iceberg_spec::PrimitiveType::Timestamp => Ok(PrimitiveType::TimestampNtz),
        iceberg_spec::PrimitiveType::Timestamptz => Ok(PrimitiveType::Timestamp),
        iceberg_spec::PrimitiveType::String => Ok(PrimitiveType::String),
        iceberg_spec::PrimitiveType::Binary => Ok(PrimitiveType::Binary),
        other => Err(Error::generic(format!(
            "Iceberg type '{}' has no Delta equivalent",
            other
        ))),
    }
}

/// Converts an Iceberg [`iceberg_spec::Type`] to a Delta [`DataType`].
fn iceberg_type_to_delta(iceberg_type: &iceberg_spec::Type) -> DeltaResult<DataType> {
    match iceberg_type {
        iceberg_spec::Type::Primitive(p) => {
            Ok(DataType::Primitive(iceberg_primitive_to_delta(p)?))
        }
        iceberg_spec::Type::Struct(s) => {
            let delta_struct = iceberg_struct_to_delta(s)?;
            Ok(DataType::Struct(Box::new(delta_struct)))
        }
        iceberg_spec::Type::List(_) => Err(Error::generic(
            "Iceberg List type is not yet supported for Delta schema conversion",
        )),
        iceberg_spec::Type::Map(_) => Err(Error::generic(
            "Iceberg Map type is not yet supported for Delta schema conversion",
        )),
    }
}

/// Converts an Iceberg [`iceberg_spec::NestedField`] to a Delta [`StructField`].
///
/// The field's column mapping ID metadata is set from the Iceberg field ID.
fn iceberg_field_to_delta(field: &iceberg_spec::NestedField) -> DeltaResult<StructField> {
    let data_type = iceberg_type_to_delta(&field.field_type)?;
    let mut delta_field = StructField::new(&field.name, data_type, !field.required);
    delta_field.metadata.insert(
        ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
        MetadataValue::Number(field.id as i64),
    );
    Ok(delta_field)
}

/// Converts an Iceberg [`iceberg_spec::StructType`] to a Delta [`StructType`].
fn iceberg_struct_to_delta(
    iceberg_struct: &iceberg_spec::StructType,
) -> DeltaResult<StructType> {
    let fields: Vec<StructField> = iceberg_struct
        .fields()
        .iter()
        .map(|f| iceberg_field_to_delta(f))
        .collect::<DeltaResult<Vec<_>>>()?;
    StructType::try_new(fields)
}

/// Converts an Iceberg [`iceberg_spec::Schema`] to a Delta [`StructType`].
///
/// Each field gets `delta.columnMapping.id` metadata set from the Iceberg field ID.
///
/// # Errors
///
/// Returns an error if:
/// - An Iceberg type with no Delta equivalent is encountered (Time, Uuid, Fixed, etc.).
/// - A list or map type is encountered (not yet supported).
pub(crate) fn iceberg_schema_to_delta(schema: &iceberg_spec::Schema) -> DeltaResult<StructType> {
    iceberg_struct_to_delta(schema.as_struct())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, PrimitiveType, StructField, StructType};

    /// Helper to create a StructField with column mapping ID metadata.
    fn field_with_id(
        name: &str,
        data_type: impl Into<DataType>,
        nullable: bool,
        id: i64,
    ) -> StructField {
        let mut f = StructField::new(name, data_type, nullable);
        f.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(id),
        );
        f
    }

    #[test]
    fn flat_schema_all_primitive_types() {
        let schema = StructType::try_new([
            field_with_id("a_string", DataType::STRING, false, 1),
            field_with_id("a_long", DataType::LONG, true, 2),
            field_with_id("an_int", DataType::INTEGER, false, 3),
            field_with_id("a_short", DataType::SHORT, true, 4),
            field_with_id("a_byte", DataType::BYTE, true, 5),
            field_with_id("a_float", DataType::FLOAT, true, 6),
            field_with_id("a_double", DataType::DOUBLE, true, 7),
            field_with_id("a_bool", DataType::BOOLEAN, false, 8),
            field_with_id("a_binary", DataType::BINARY, true, 9),
            field_with_id("a_date", DataType::DATE, true, 10),
            field_with_id("a_timestamp", DataType::TIMESTAMP, true, 11),
            field_with_id("a_timestamp_ntz", DataType::TIMESTAMP_NTZ, true, 12),
            field_with_id(
                "a_decimal",
                PrimitiveType::Decimal(
                    crate::schema::DecimalType::try_new(10, 2).unwrap(),
                ),
                true,
                13,
            ),
        ])
        .unwrap();

        let iceberg = delta_schema_to_iceberg(&schema, 0, vec![]).unwrap();
        let json = serde_json::to_value(&iceberg).unwrap();

        let fields = json["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 13);

        assert_eq!(fields[0]["type"], "string");
        assert_eq!(fields[0]["required"], true);
        assert_eq!(fields[0]["id"], 1);

        assert_eq!(fields[1]["type"], "long");
        assert_eq!(fields[1]["required"], false);

        assert_eq!(fields[2]["type"], "int");
        assert_eq!(fields[3]["type"], "int"); // short -> int
        assert_eq!(fields[4]["type"], "int"); // byte -> int

        assert_eq!(fields[5]["type"], "float");
        assert_eq!(fields[6]["type"], "double");
        assert_eq!(fields[7]["type"], "boolean");
        assert_eq!(fields[8]["type"], "binary");
        assert_eq!(fields[9]["type"], "date");
        assert_eq!(fields[10]["type"], "timestamptz");
        assert_eq!(fields[11]["type"], "timestamp");
        assert_eq!(fields[12]["type"], "decimal(10,2)");
    }

    #[test]
    fn nested_struct() {
        let inner = StructType::try_new([
            field_with_id("x", DataType::INTEGER, false, 3),
            field_with_id("y", DataType::INTEGER, false, 4),
        ])
        .unwrap();
        let schema = StructType::try_new([
            field_with_id("id", DataType::LONG, false, 1),
            field_with_id("point", inner, true, 2),
        ])
        .unwrap();

        let iceberg = delta_schema_to_iceberg(&schema, 0, vec![]).unwrap();
        let json = serde_json::to_value(&iceberg).unwrap();

        let point_type = &json["fields"][1]["type"];
        assert_eq!(point_type["type"], "struct");

        let inner_fields = point_type["fields"].as_array().unwrap();
        assert_eq!(inner_fields.len(), 2);
        assert_eq!(inner_fields[0]["id"], 3);
        assert_eq!(inner_fields[0]["name"], "x");
        assert_eq!(inner_fields[1]["id"], 4);
        assert_eq!(inner_fields[1]["name"], "y");
    }

    #[test]
    fn schema_id_and_identifier_fields() {
        let schema = StructType::try_new([
            field_with_id("id", DataType::LONG, false, 1),
            field_with_id("name", DataType::STRING, false, 2),
        ])
        .unwrap();

        let iceberg = delta_schema_to_iceberg(&schema, 5, vec![1]).unwrap();
        let json = serde_json::to_value(&iceberg).unwrap();

        assert_eq!(json["schema-id"], 5);
        assert_eq!(json["identifier-field-ids"], serde_json::json!([1]));
    }

    #[test]
    fn missing_field_id_returns_error() {
        let schema =
            StructType::try_new([StructField::nullable("no_id", DataType::STRING)]).unwrap();

        let result = delta_schema_to_iceberg(&schema, 0, vec![]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("no_id") && err.contains("columnMapping.id"),
            "Error should mention field name and missing metadata, got: {}",
            err
        );
    }

    #[test]
    fn variant_type_returns_error() {
        let variant_schema = StructType::try_new([
            StructField::not_null("metadata", DataType::BINARY),
            StructField::not_null("value", DataType::BINARY),
        ])
        .unwrap();
        let schema = StructType::try_new([field_with_id(
            "v",
            DataType::Variant(Box::new(variant_schema)),
            true,
            1,
        )])
        .unwrap();

        let result = delta_schema_to_iceberg(&schema, 0, vec![]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Variant"));
    }

    #[test]
    fn full_json_output_matches_expected_format() {
        let schema = StructType::try_new([
            field_with_id("id", DataType::INTEGER, false, 1),
            field_with_id("data", DataType::STRING, true, 2),
        ])
        .unwrap();

        let iceberg = delta_schema_to_iceberg(&schema, 0, vec![]).unwrap();
        let json = serde_json::to_value(&iceberg).unwrap();

        // Iceberg Schema omits identifier-field-ids when empty
        assert_eq!(json["type"], "struct");
        assert_eq!(json["schema-id"], 0);
        assert!(json.get("identifier-field-ids").is_none());

        let fields = json["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], serde_json::json!({"id": 1, "name": "id", "required": true, "type": "int"}));
        assert_eq!(fields[1], serde_json::json!({"id": 2, "name": "data", "required": false, "type": "string"}));
    }

    // -----------------------------------------------------------------------
    // Iceberg -> Delta tests
    // -----------------------------------------------------------------------

    #[test]
    fn iceberg_to_delta_primitive_types() {
        use std::sync::Arc;

        let iceberg_schema = iceberg_spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(iceberg_spec::NestedField::required(
                    1, "a_bool", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Boolean),
                )),
                Arc::new(iceberg_spec::NestedField::optional(
                    2, "a_int", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Int),
                )),
                Arc::new(iceberg_spec::NestedField::required(
                    3, "a_long", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Long),
                )),
                Arc::new(iceberg_spec::NestedField::optional(
                    4, "a_float", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Float),
                )),
                Arc::new(iceberg_spec::NestedField::optional(
                    5, "a_double", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Double),
                )),
                Arc::new(iceberg_spec::NestedField::optional(
                    6, "a_string", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::String),
                )),
                Arc::new(iceberg_spec::NestedField::optional(
                    7, "a_binary", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Binary),
                )),
                Arc::new(iceberg_spec::NestedField::optional(
                    8, "a_date", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Date),
                )),
                Arc::new(iceberg_spec::NestedField::optional(
                    9, "a_timestamptz", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Timestamptz),
                )),
                Arc::new(iceberg_spec::NestedField::optional(
                    10, "a_timestamp", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Timestamp),
                )),
                Arc::new(iceberg_spec::NestedField::optional(
                    11, "a_decimal", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Decimal {
                        precision: 10,
                        scale: 2,
                    }),
                )),
            ])
            .build()
            .unwrap();

        let delta = iceberg_schema_to_delta(&iceberg_schema).unwrap();
        let fields: Vec<&StructField> = delta.fields().collect();

        assert_eq!(fields.len(), 11);

        // Check types
        assert_eq!(fields[0].data_type, DataType::BOOLEAN);
        assert!(!fields[0].nullable); // required
        assert_eq!(fields[1].data_type, DataType::INTEGER);
        assert!(fields[1].nullable); // optional
        assert_eq!(fields[2].data_type, DataType::LONG);
        assert_eq!(fields[3].data_type, DataType::FLOAT);
        assert_eq!(fields[4].data_type, DataType::DOUBLE);
        assert_eq!(fields[5].data_type, DataType::STRING);
        assert_eq!(fields[6].data_type, DataType::BINARY);
        assert_eq!(fields[7].data_type, DataType::DATE);
        assert_eq!(fields[8].data_type, DataType::TIMESTAMP); // timestamptz -> Timestamp
        assert_eq!(fields[9].data_type, DataType::TIMESTAMP_NTZ); // timestamp -> TimestampNtz
        assert_eq!(fields[10].data_type, DataType::Primitive(PrimitiveType::Decimal(
            crate::schema::DecimalType::try_new(10, 2).unwrap(),
        )));

        // Check field IDs in metadata
        for (i, field) in fields.iter().enumerate() {
            let id = field
                .get_config_value(&ColumnMetadataKey::ColumnMappingId)
                .unwrap();
            assert_eq!(*id, MetadataValue::Number((i + 1) as i64));
        }
    }

    #[test]
    fn iceberg_to_delta_nested_struct() {
        use std::sync::Arc;

        let inner_struct = iceberg_spec::StructType::new(vec![
            Arc::new(iceberg_spec::NestedField::required(
                3, "x", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Int),
            )),
            Arc::new(iceberg_spec::NestedField::required(
                4, "y", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Int),
            )),
        ]);

        let iceberg_schema = iceberg_spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(iceberg_spec::NestedField::required(
                    1, "id", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Long),
                )),
                Arc::new(iceberg_spec::NestedField::optional(
                    2, "point", iceberg_spec::Type::Struct(inner_struct),
                )),
            ])
            .build()
            .unwrap();

        let delta = iceberg_schema_to_delta(&iceberg_schema).unwrap();
        let fields: Vec<&StructField> = delta.fields().collect();

        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[1].name, "point");
        assert!(fields[1].nullable);

        // Check nested struct
        if let DataType::Struct(inner) = &fields[1].data_type {
            let inner_fields: Vec<&StructField> = inner.fields().collect();
            assert_eq!(inner_fields.len(), 2);
            assert_eq!(inner_fields[0].name, "x");
            assert_eq!(inner_fields[1].name, "y");
            // Check inner field IDs
            assert_eq!(
                *inner_fields[0].get_config_value(&ColumnMetadataKey::ColumnMappingId).unwrap(),
                MetadataValue::Number(3)
            );
            assert_eq!(
                *inner_fields[1].get_config_value(&ColumnMetadataKey::ColumnMappingId).unwrap(),
                MetadataValue::Number(4)
            );
        } else {
            panic!("Expected Struct type for 'point' field");
        }
    }

    #[test]
    fn iceberg_to_delta_unsupported_type_returns_error() {
        use std::sync::Arc;

        let iceberg_schema = iceberg_spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(iceberg_spec::NestedField::optional(
                1, "t", iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Time),
            ))])
            .build()
            .unwrap();

        let result = iceberg_schema_to_delta(&iceberg_schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no Delta equivalent"));
    }

    #[test]
    fn round_trip_delta_to_iceberg_and_back() {
        let original = StructType::try_new([
            field_with_id("id", DataType::LONG, false, 1),
            field_with_id("name", DataType::STRING, true, 2),
            field_with_id("score", DataType::DOUBLE, true, 3),
        ])
        .unwrap();

        let iceberg = delta_schema_to_iceberg(&original, 0, vec![]).unwrap();
        let round_tripped = iceberg_schema_to_delta(&iceberg).unwrap();

        // Field names, types, and nullability should match
        let orig_fields: Vec<&StructField> = original.fields().collect();
        let rt_fields: Vec<&StructField> = round_tripped.fields().collect();
        assert_eq!(orig_fields.len(), rt_fields.len());
        for (o, r) in orig_fields.iter().zip(rt_fields.iter()) {
            assert_eq!(o.name, r.name);
            assert_eq!(o.data_type, r.data_type);
            assert_eq!(o.nullable, r.nullable);
        }
    }
}
