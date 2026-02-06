use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use crate::arrow::array::cast::AsArray;
use crate::arrow::array::types::{Int32Type, Int64Type};
use crate::arrow::array::{
    Array, ArrayRef, GenericListArray, MapArray, OffsetSizeTrait, RecordBatch, StructArray,
};
use crate::arrow::compute::filter_record_batch;
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef, Schema as ArrowSchema,
};
use crate::engine_data::{EngineData, EngineList, EngineMap, EngineStruct, GetData, RowVisitor};
use crate::expressions::{ArrayData, Scalar, StructData};
use crate::schema::{ColumnName, DataType, SchemaRef, StructField};
use crate::{DeltaResult, Error};

pub use crate::engine::arrow_utils::fix_nested_null_masks;

/// ArrowEngineData holds an Arrow `RecordBatch`, implements `EngineData` so the kernel can extract from it.
///
/// WARNING: Row visitors require that all leaf columns of the record batch have correctly computed
/// NULL masks. The arrow parquet reader is known to produce incomplete NULL masks, for
/// example. When in doubt, call [`fix_nested_null_masks`] first.
pub struct ArrowEngineData {
    data: RecordBatch,
}

/// A trait to allow easy conversion from [`EngineData`] to an arrow [``RecordBatch`]. Returns an
/// error if called on an `EngineData` that is not an `ArrowEngineData`.
pub trait EngineDataArrowExt {
    fn try_into_record_batch(self) -> DeltaResult<RecordBatch>;
}

impl EngineDataArrowExt for Box<dyn EngineData> {
    fn try_into_record_batch(self) -> DeltaResult<RecordBatch> {
        Ok(self
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
            .into())
    }
}

impl EngineDataArrowExt for DeltaResult<Box<dyn EngineData>> {
    fn try_into_record_batch(self) -> DeltaResult<RecordBatch> {
        Ok(self?
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
            .into())
    }
}

/// Helper function to extract a RecordBatch from EngineData, ensuring it's ArrowEngineData
pub(crate) fn extract_record_batch(engine_data: &dyn EngineData) -> DeltaResult<&RecordBatch> {
    let Some(arrow_data) = engine_data.any_ref().downcast_ref::<ArrowEngineData>() else {
        return Err(Error::engine_data_type("ArrowEngineData"));
    };
    Ok(arrow_data.record_batch())
}

/// unshredded variant arrow type: struct of two non-nullable binary fields 'metadata' and 'value'
#[allow(dead_code)]
pub(crate) fn unshredded_variant_arrow_type() -> ArrowDataType {
    let metadata_field = ArrowField::new("metadata", ArrowDataType::Binary, false);
    let value_field = ArrowField::new("value", ArrowDataType::Binary, false);
    let fields = vec![metadata_field, value_field];
    ArrowDataType::Struct(fields.into())
}

impl ArrowEngineData {
    /// Create a new `ArrowEngineData` from a `RecordBatch`
    pub fn new(data: RecordBatch) -> Self {
        ArrowEngineData { data }
    }

    /// Utility constructor to get a `Box<ArrowEngineData>` out of a `Box<dyn EngineData>`
    pub fn try_from_engine_data(engine_data: Box<dyn EngineData>) -> DeltaResult<Box<Self>> {
        engine_data
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| Error::engine_data_type("ArrowEngineData"))
    }

    /// Get a reference to the `RecordBatch` this `ArrowEngineData` is wrapping
    pub fn record_batch(&self) -> &RecordBatch {
        &self.data
    }
}

impl From<RecordBatch> for ArrowEngineData {
    fn from(value: RecordBatch) -> Self {
        ArrowEngineData::new(value)
    }
}

impl From<StructArray> for ArrowEngineData {
    fn from(value: StructArray) -> Self {
        ArrowEngineData::new(value.into())
    }
}

impl From<ArrowEngineData> for RecordBatch {
    fn from(value: ArrowEngineData) -> Self {
        value.data
    }
}

impl From<Box<ArrowEngineData>> for RecordBatch {
    fn from(value: Box<ArrowEngineData>) -> Self {
        value.data
    }
}

impl<OffsetSize> EngineList for GenericListArray<OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
{
    fn len(&self, row_index: usize) -> usize {
        self.value(row_index).len()
    }

    fn get(&self, row_index: usize, index: usize) -> String {
        let arry = self.value(row_index);
        let sarry = arry.as_string::<i32>();
        sarry.value(index).to_string()
    }

    fn materialize(&self, row_index: usize) -> Vec<String> {
        let mut result = vec![];
        for i in 0..EngineList::len(self, row_index) {
            result.push(self.get(row_index, i));
        }
        result
    }
}

impl EngineMap for MapArray {
    fn get<'a>(&'a self, row_index: usize, key: &str) -> Option<&'a str> {
        let offsets = self.offsets();
        let start_offset = offsets[row_index] as usize;
        let count = offsets[row_index + 1] as usize - start_offset;
        let keys = self.keys().as_string::<i32>();
        for (idx, map_key) in keys.iter().enumerate().skip(start_offset).take(count) {
            if let Some(map_key) = map_key {
                if key == map_key {
                    // found the item
                    let vals = self.values().as_string::<i32>();
                    return Some(vals.value(idx));
                }
            }
        }
        None
    }

    fn materialize(&self, row_index: usize) -> HashMap<String, String> {
        let mut ret = HashMap::new();
        let map_val = self.value(row_index);
        let keys = map_val.column(0).as_string::<i32>();
        let values = map_val.column(1).as_string::<i32>();
        for (key, value) in keys.iter().zip(values.iter()) {
            if let (Some(key), Some(value)) = (key, value) {
                ret.insert(key.into(), value.into());
            }
        }
        ret
    }
}

/// Converts an Arrow array value at a given index to a Scalar.
fn arrow_value_to_scalar(array: &dyn Array, row_index: usize) -> DeltaResult<Scalar> {
    use crate::arrow::array::types::{Float32Type, Float64Type, Int16Type, Int8Type};
    use crate::arrow::datatypes::DataType as ArrowDataType;

    if !array.is_valid(row_index) {
        let data_type = arrow_data_type_to_kernel_data_type(array.data_type())?;
        return Ok(Scalar::Null(data_type));
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_boolean();
            Ok(Scalar::Boolean(arr.value(row_index)))
        }
        ArrowDataType::Int8 => {
            let arr = array.as_primitive::<Int8Type>();
            Ok(Scalar::Byte(arr.value(row_index)))
        }
        ArrowDataType::Int16 => {
            let arr = array.as_primitive::<Int16Type>();
            Ok(Scalar::Short(arr.value(row_index)))
        }
        ArrowDataType::Int32 => {
            let arr = array.as_primitive::<Int32Type>();
            Ok(Scalar::Integer(arr.value(row_index)))
        }
        ArrowDataType::Int64 => {
            let arr = array.as_primitive::<Int64Type>();
            Ok(Scalar::Long(arr.value(row_index)))
        }
        ArrowDataType::Float32 => {
            let arr = array.as_primitive::<Float32Type>();
            Ok(Scalar::Float(arr.value(row_index)))
        }
        ArrowDataType::Float64 => {
            let arr = array.as_primitive::<Float64Type>();
            Ok(Scalar::Double(arr.value(row_index)))
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_string::<i32>();
            Ok(Scalar::String(arr.value(row_index).to_string()))
        }
        ArrowDataType::Binary => {
            let arr = array.as_binary::<i32>();
            Ok(Scalar::Binary(arr.value(row_index).to_vec()))
        }
        ArrowDataType::Date32 => {
            use crate::arrow::array::types::Date32Type;
            let arr = array.as_primitive::<Date32Type>();
            Ok(Scalar::Date(arr.value(row_index)))
        }
        ArrowDataType::Timestamp(unit, _) => {
            use crate::arrow::array::types::{
                TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
                TimestampSecondType,
            };
            use crate::arrow::datatypes::TimeUnit;
            let micros = match unit {
                TimeUnit::Second => {
                    array.as_primitive::<TimestampSecondType>().value(row_index) * 1_000_000
                }
                TimeUnit::Millisecond => {
                    array
                        .as_primitive::<TimestampMillisecondType>()
                        .value(row_index)
                        * 1_000
                }
                TimeUnit::Microsecond => array
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(row_index),
                TimeUnit::Nanosecond => {
                    array
                        .as_primitive::<TimestampNanosecondType>()
                        .value(row_index)
                        / 1_000
                }
            };
            Ok(Scalar::Timestamp(micros))
        }
        ArrowDataType::Struct(_) => {
            let struct_arr = array.as_struct();
            struct_array_row_to_struct_data(struct_arr, row_index).map(Scalar::Struct)
        }
        other => Err(Error::generic(format!(
            "Unsupported Arrow type for Scalar conversion: {:?}",
            other
        ))),
    }
}

/// Converts an Arrow DataType to a kernel DataType.
fn arrow_data_type_to_kernel_data_type(
    arrow_type: &crate::arrow::datatypes::DataType,
) -> DeltaResult<DataType> {
    use crate::arrow::datatypes::DataType as ArrowDataType;
    use crate::schema::{ArrayType, MapType, StructType};

    match arrow_type {
        ArrowDataType::Boolean => Ok(DataType::BOOLEAN),
        ArrowDataType::Int8 => Ok(DataType::BYTE),
        ArrowDataType::Int16 => Ok(DataType::SHORT),
        ArrowDataType::Int32 => Ok(DataType::INTEGER),
        ArrowDataType::Int64 => Ok(DataType::LONG),
        ArrowDataType::Float32 => Ok(DataType::FLOAT),
        ArrowDataType::Float64 => Ok(DataType::DOUBLE),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(DataType::STRING),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(DataType::BINARY),
        ArrowDataType::Date32 => Ok(DataType::DATE),
        ArrowDataType::Timestamp(_, Some(_)) => Ok(DataType::TIMESTAMP),
        ArrowDataType::Timestamp(_, None) => Ok(DataType::TIMESTAMP_NTZ),
        ArrowDataType::Struct(fields) => {
            let kernel_fields: DeltaResult<Vec<StructField>> = fields
                .iter()
                .map(|f| {
                    let dt = arrow_data_type_to_kernel_data_type(f.data_type())?;
                    Ok(StructField::new(f.name(), dt, f.is_nullable()))
                })
                .collect();
            Ok(DataType::Struct(Box::new(StructType::new_unchecked(
                kernel_fields?,
            ))))
        }
        ArrowDataType::List(field) | ArrowDataType::LargeList(field) => {
            let element_type = arrow_data_type_to_kernel_data_type(field.data_type())?;
            Ok(DataType::Array(Box::new(ArrayType::new(
                element_type,
                field.is_nullable(),
            ))))
        }
        ArrowDataType::Map(field, _) => {
            if let ArrowDataType::Struct(fields) = field.data_type() {
                let key_type = arrow_data_type_to_kernel_data_type(fields[0].data_type())?;
                let value_type = arrow_data_type_to_kernel_data_type(fields[1].data_type())?;
                Ok(DataType::Map(Box::new(MapType::new(
                    key_type,
                    value_type,
                    fields[1].is_nullable(),
                ))))
            } else {
                Err(Error::generic("Map field must be a struct"))
            }
        }
        ArrowDataType::Null => Ok(DataType::STRING), // Use STRING as a placeholder for null type
        other => Err(Error::generic(format!(
            "Unsupported Arrow type: {:?}",
            other
        ))),
    }
}

/// Materializes a StructArray row at the given index into a StructData.
fn struct_array_row_to_struct_data(
    struct_array: &StructArray,
    row_index: usize,
) -> DeltaResult<StructData> {
    let arrow_fields = struct_array.fields();
    let columns = struct_array.columns();

    let mut fields = Vec::with_capacity(arrow_fields.len());
    let mut values = Vec::with_capacity(arrow_fields.len());

    for (arrow_field, column) in arrow_fields.iter().zip(columns.iter()) {
        let data_type = arrow_data_type_to_kernel_data_type(arrow_field.data_type())?;
        let field = StructField::new(arrow_field.name(), data_type, arrow_field.is_nullable());
        let value = arrow_value_to_scalar(column.as_ref(), row_index)?;

        fields.push(field);
        values.push(value);
    }

    StructData::try_new(fields, values)
}

impl EngineStruct for StructArray {
    fn materialize(&self, row_index: usize) -> DeltaResult<StructData> {
        struct_array_row_to_struct_data(self, row_index)
    }
}

/// Helper trait that provides uniform access to columns and fields, so that our row visitor can use
/// the same code to drill into a `RecordBatch` (initial case) or `StructArray` (nested case).
trait ProvidesColumnsAndFields {
    fn columns(&self) -> &[ArrayRef];
    fn fields(&self) -> &[FieldRef];
}

impl ProvidesColumnsAndFields for RecordBatch {
    fn columns(&self) -> &[ArrayRef] {
        self.columns()
    }
    fn fields(&self) -> &[FieldRef] {
        self.schema_ref().fields()
    }
}

impl ProvidesColumnsAndFields for StructArray {
    fn columns(&self) -> &[ArrayRef] {
        self.columns()
    }
    fn fields(&self) -> &[FieldRef] {
        self.fields()
    }
}

impl EngineData for ArrowEngineData {
    fn len(&self) -> usize {
        self.data.num_rows()
    }

    fn visit_rows(
        &self,
        leaf_columns: &[ColumnName],
        visitor: &mut dyn RowVisitor,
    ) -> DeltaResult<()> {
        // Make sure the caller passed the correct number of column names
        let leaf_types = visitor.selected_column_names_and_types().1;
        if leaf_types.len() != leaf_columns.len() {
            return Err(Error::MissingColumn(format!(
                "Visitor expected {} column names, but caller passed {}",
                leaf_types.len(),
                leaf_columns.len()
            ))
            .with_backtrace());
        }

        // Build a map of column paths to their expected types.
        // - For parent paths (non-leaf), the value is None (used for traversal into nested structs)
        // - For leaf columns, the value is Some(&DataType) (used for type validation)
        // This allows extract_columns to look up the expected type by column name instead of
        // using positional indexing, which fixes bugs when columns are found in different order
        // than they were requested.
        let mut column_map = HashMap::new();

        // Add all parent paths with None (for traversal)
        for column in leaf_columns {
            for i in 0..(column.len()) {
                column_map.entry(&column[..i + 1]).or_insert(None);
            }
        }

        // Set leaf columns to Some(data_type)
        for (column, data_type) in leaf_columns.iter().zip(leaf_types.iter()) {
            column_map.insert(column.as_ref(), Some(data_type));
        }
        debug!(
            "Column map for selected columns {leaf_columns:?} has {} entries",
            column_map.len()
        );

        let mut getters = vec![];
        Self::extract_columns(&mut vec![], &mut getters, &column_map, &self.data)?;
        if getters.len() != leaf_columns.len() {
            return Err(Error::MissingColumn(format!(
                "Visitor expected {} leaf columns, but only {} were found in the data",
                leaf_columns.len(),
                getters.len()
            )));
        }
        visitor.visit(self.len(), &getters)
    }

    fn append_columns(
        &self,
        schema: SchemaRef,
        columns: Vec<ArrayData>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        use crate::arrow::array::{make_builder, ArrayBuilder};
        use crate::engine::arrow_conversion::{TryFromKernel, TryIntoArrow};

        // Combine existing and new schema fields
        let schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
        let mut combined_fields = self.data.schema().fields().to_vec();
        combined_fields.extend_from_slice(schema.fields());
        let combined_schema = Arc::new(ArrowSchema::new(combined_fields));

        // Combine existing and new columns
        // Convert kernel ArrayData to Arrow arrays
        let new_columns: Vec<ArrayRef> = columns
            .into_iter()
            .map(|array_data| {
                let elements = array_data.array_elements();

                // Get the element type from the ArrayType
                let element_type = array_data.array_type().element_type();
                let arrow_data_type = ArrowDataType::try_from_kernel(element_type)?;

                // Create a builder and append each scalar
                let mut builder = make_builder(&arrow_data_type, elements.len());
                for scalar in elements {
                    scalar.append_to(&mut *builder, 1)?;
                }

                Ok(builder.finish())
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        let mut combined_columns = self.data.columns().to_vec();
        combined_columns.extend(new_columns);

        // Create a new ArrowEngineData with the combined schema and columns
        let data = RecordBatch::try_new(combined_schema, combined_columns)?;
        Ok(Box::new(ArrowEngineData { data }))
    }

    fn apply_selection_vector(
        self: Box<Self>,
        mut selection_vector: Vec<bool>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        selection_vector.resize(self.len(), true);
        let filtered = filter_record_batch(&self.data, &selection_vector.into())?;
        Ok(Box::new(Self::new(filtered)))
    }
}

impl ArrowEngineData {
    fn extract_columns<'a>(
        path: &mut Vec<String>,
        getters: &mut Vec<&'a dyn GetData<'a>>,
        column_map: &HashMap<&[String], Option<&DataType>>,
        data: &'a dyn ProvidesColumnsAndFields,
    ) -> DeltaResult<()> {
        for (column, field) in data.columns().iter().zip(data.fields()) {
            path.push(field.name().to_string());
            if let Some(type_option) = column_map.get(&path[..]) {
                if let Some(struct_array) = column.as_struct_opt() {
                    // Check if the expected type is Struct - if so, extract the struct as a whole
                    // Otherwise, recurse into the struct to extract nested fields
                    if matches!(type_option, Some(DataType::Struct(_))) {
                        debug!("Pushing struct array for {}", ColumnName::new(path.iter()));
                        getters.push(struct_array);
                    } else {
                        debug!(
                            "Recurse into a struct array for {}",
                            ColumnName::new(path.iter())
                        );
                        Self::extract_columns(path, getters, column_map, struct_array)?;
                    }
                } else if column.data_type() == &ArrowDataType::Null {
                    debug!("Pushing a null array for {}", ColumnName::new(path.iter()));
                    getters.push(&());
                } else if let Some(data_type) = type_option {
                    // Leaf column with expected type - look up type by name instead of position
                    let getter = Self::extract_leaf_column(path, data_type, column)?;
                    getters.push(getter);
                }
                // If type_option is None, it's a parent path with no leaf to extract - skip
            } else {
                debug!("Skipping unmasked path {}", ColumnName::new(path.iter()));
            }
            path.pop();
        }
        Ok(())
    }

    fn extract_leaf_column<'a>(
        path: &[String],
        data_type: &DataType,
        col: &'a dyn Array,
    ) -> DeltaResult<&'a dyn GetData<'a>> {
        use ArrowDataType::Utf8;
        let col_as_list = || {
            if let Some(array) = col.as_list_opt::<i32>() {
                (array.value_type() == Utf8).then_some(array as _)
            } else if let Some(array) = col.as_list_opt::<i64>() {
                (array.value_type() == Utf8).then_some(array as _)
            } else {
                None
            }
        };
        let col_as_map = || {
            col.as_map_opt().and_then(|array| {
                (array.key_type() == &Utf8 && array.value_type() == &Utf8).then_some(array as _)
            })
        };
        let result: Result<&'a dyn GetData<'a>, _> = match data_type {
            &DataType::BOOLEAN => {
                debug!("Pushing boolean array for {}", ColumnName::new(path));
                col.as_boolean_opt().map(|a| a as _).ok_or("bool")
            }
            &DataType::STRING => {
                debug!("Pushing string array for {}", ColumnName::new(path));
                col.as_string_opt().map(|a| a as _).ok_or("string")
            }
            &DataType::BINARY => {
                debug!("Pushing binary array for {}", ColumnName::new(path));
                col.as_binary_opt().map(|a| a as _).ok_or("binary")
            }
            &DataType::INTEGER => {
                debug!("Pushing int32 array for {}", ColumnName::new(path));
                col.as_primitive_opt::<Int32Type>()
                    .map(|a| a as _)
                    .ok_or("int")
            }
            &DataType::LONG => {
                debug!("Pushing int64 array for {}", ColumnName::new(path));
                col.as_primitive_opt::<Int64Type>()
                    .map(|a| a as _)
                    .ok_or("long")
            }
            DataType::Array(_) => {
                debug!("Pushing list for {}", ColumnName::new(path));
                col_as_list().ok_or("array<string>")
            }
            DataType::Map(_) => {
                debug!("Pushing map for {}", ColumnName::new(path));
                col_as_map().ok_or("map<string, string>")
            }
            data_type => {
                return Err(Error::UnexpectedColumnType(format!(
                    "On {}: Unsupported type {data_type}",
                    ColumnName::new(path)
                )));
            }
        };
        result.map_err(|type_name| {
            Error::UnexpectedColumnType(format!(
                "Type mismatch on {}: expected {}, got {}",
                ColumnName::new(path),
                type_name,
                col.data_type()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::actions::{get_commit_schema, Metadata, Protocol};
    use crate::arrow::array::types::Int32Type;
    use crate::arrow::array::{Array, AsArray, Int32Array, RecordBatch, StringArray};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::engine::sync::SyncEngine;
    use crate::expressions::ArrayData;
    use crate::schema::{ArrayType, DataType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::{assert_result_error_with_message, string_array_to_engine_data};
    use crate::{DeltaResult, Engine as _, EngineData as _};

    use super::{extract_record_batch, ArrowEngineData};

    #[test]
    fn test_md_extract() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let handler = engine.json_handler();
        let json_strings: StringArray = vec![
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
        ]
        .into();
        let output_schema = get_commit_schema().clone();
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let metadata = Metadata::try_new_from_data(parsed.as_ref())?.unwrap();
        assert_eq!(metadata.id(), "aff5cb91-8cd9-4195-aef9-446908507302");
        assert_eq!(metadata.created_time(), Some(1670892997849));
        assert_eq!(*metadata.partition_columns(), vec!("c1", "c2"));
        Ok(())
    }

    #[test]
    fn test_protocol_extract() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let handler = engine.json_handler();
        let json_strings: StringArray = vec![
            r#"{"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["rw1"], "writerFeatures": ["rw1", "w2"]}}"#,
        ]
        .into();
        let output_schema = get_commit_schema().project(&["protocol"])?;
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let protocol = Protocol::try_new_from_data(parsed.as_ref())?.unwrap();
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert_eq!(
            protocol.reader_features(),
            Some([TableFeature::unknown("rw1")].as_slice())
        );
        assert_eq!(
            protocol.writer_features(),
            Some([TableFeature::unknown("rw1"), TableFeature::unknown("w2")].as_slice())
        );
        Ok(())
    }

    #[test]
    fn test_append_columns() -> DeltaResult<()> {
        // Create initial ArrowEngineData with 2 rows and 2 columns
        let initial_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let initial_batch = RecordBatch::try_new(
            initial_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
            ],
        )?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        // Create new columns as ArrayData
        let new_columns = vec![
            ArrayData::try_new(
                ArrayType::new(DataType::INTEGER, true),
                vec![Some(25), None],
            )?,
            ArrayData::try_new(ArrayType::new(DataType::BOOLEAN, false), vec![true, false])?,
        ];

        // Create schema for the new columns
        let new_schema = Arc::new(StructType::new_unchecked([
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("active", DataType::BOOLEAN, false),
        ]));

        // Test the append_columns method
        let arrow_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(arrow_data.as_ref())?;

        // Verify the result
        assert_eq!(result_batch.num_columns(), 4);
        assert_eq!(result_batch.num_rows(), 2);

        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "age");
        assert_eq!(schema.field(3).name(), "active");

        assert_eq!(schema.field(0).data_type(), &ArrowDataType::Int32);
        assert_eq!(schema.field(1).data_type(), &ArrowDataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &ArrowDataType::Int32);
        assert_eq!(schema.field(3).data_type(), &ArrowDataType::Boolean);

        let id_column = result_batch.column(0).as_primitive::<Int32Type>();
        let name_column = result_batch.column(1).as_string::<i32>();
        let age_column = result_batch.column(2).as_primitive::<Int32Type>();
        let active_column = result_batch.column(3).as_boolean();

        assert_eq!(id_column.values(), &[1, 2]);
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");
        assert_eq!(age_column.value(0), 25);
        assert!(age_column.is_null(1));
        assert!(active_column.value(0));
        assert!(!active_column.value(1));

        Ok(())
    }

    #[test]
    fn test_append_columns_row_mismatch() -> DeltaResult<()> {
        // Create initial ArrowEngineData with 2 rows
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch =
            RecordBatch::try_new(initial_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
        let arrow_data = super::ArrowEngineData::new(initial_batch);

        // Create new column with wrong number of rows (3 instead of 2)
        let new_columns = vec![ArrayData::try_new(
            ArrayType::new(DataType::INTEGER, false),
            vec![25, 30, 35],
        )?];

        let new_schema = Arc::new(StructType::new_unchecked([StructField::new(
            "age",
            DataType::INTEGER,
            true,
        )]));

        let result = arrow_data.append_columns(new_schema, new_columns);
        assert_result_error_with_message(
            result,
            "all columns in a record batch must have the same length",
        );

        Ok(())
    }

    #[test]
    fn test_append_columns_schema_field_count_mismatch() -> DeltaResult<()> {
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch =
            RecordBatch::try_new(initial_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        // Schema has 2 fields but only 1 column provided
        let new_columns = vec![ArrayData::try_new(
            ArrayType::new(DataType::STRING, true),
            vec![Some("Alice".to_string()), Some("Bob".to_string())],
        )?];

        let new_schema = Arc::new(StructType::new_unchecked([
            StructField::new("name", DataType::STRING, true),
            StructField::new("email", DataType::STRING, true), // Extra field in schema
        ]));

        let result = arrow_data.append_columns(new_schema, new_columns);
        assert_result_error_with_message(
            result,
            "number of columns(2) must match number of fields(3)",
        );

        Ok(())
    }

    #[test]
    fn test_append_columns_empty_existing_data() -> DeltaResult<()> {
        // Create empty ArrowEngineData with schema but no rows
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch = RecordBatch::try_new(
            initial_schema,
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        )?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        // Create empty new columns
        let new_columns = vec![ArrayData::try_new(
            ArrayType::new(DataType::STRING, true),
            Vec::<Option<String>>::new(),
        )?];
        let new_schema = Arc::new(StructType::new_unchecked([StructField::new(
            "name",
            DataType::STRING,
            true,
        )]));

        let result_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(result_data.as_ref())?;

        assert_eq!(result_batch.num_columns(), 2);
        assert_eq!(result_batch.num_rows(), 0);
        assert_eq!(result_batch.schema().field(0).name(), "id");
        assert_eq!(result_batch.schema().field(1).name(), "name");

        Ok(())
    }

    #[test]
    fn test_append_columns_empty_new_columns() -> DeltaResult<()> {
        // Create ArrowEngineData with some data
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch =
            RecordBatch::try_new(initial_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        // Create empty schema and columns
        let new_columns = vec![];
        let new_schema = Arc::new(StructType::new_unchecked([]));

        let result_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(result_data.as_ref())?;

        // Should be identical to original
        assert_eq!(result_batch.num_columns(), 1);
        assert_eq!(result_batch.num_rows(), 2);
        assert_eq!(result_batch.schema().field(0).name(), "id");

        Ok(())
    }

    #[test]
    fn test_append_columns_with_nulls() -> DeltaResult<()> {
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch = RecordBatch::try_new(
            initial_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        let new_columns = vec![
            ArrayData::try_new(
                ArrayType::new(DataType::STRING, true),
                vec![Some("Alice".to_string()), None, Some("Charlie".to_string())],
            )?,
            ArrayData::try_new(
                ArrayType::new(DataType::INTEGER, true),
                vec![Some(25), Some(30), None],
            )?,
        ];

        let new_schema = Arc::new(StructType::new_unchecked([
            StructField::new("name", DataType::STRING, true),
            StructField::new("age", DataType::INTEGER, true),
        ]));

        let result_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(result_data.as_ref())?;

        assert_eq!(result_batch.num_columns(), 3);
        assert_eq!(result_batch.num_rows(), 3);

        // Verify nullable columns work correctly
        assert!(!result_batch.schema().field(0).is_nullable());
        assert!(result_batch.schema().field(1).is_nullable());
        assert!(result_batch.schema().field(2).is_nullable());

        Ok(())
    }

    #[test]
    fn test_append_columns_various_data_types() -> DeltaResult<()> {
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch =
            RecordBatch::try_new(initial_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        let new_columns = vec![
            ArrayData::try_new(
                ArrayType::new(DataType::LONG, false),
                vec![1000_i64, 2000_i64],
            )?,
            ArrayData::try_new(
                ArrayType::new(DataType::DOUBLE, true),
                vec![Some(3.87), Some(2.71)],
            )?,
            ArrayData::try_new(ArrayType::new(DataType::BOOLEAN, false), vec![true, false])?,
        ];

        let new_schema = Arc::new(StructType::new_unchecked([
            StructField::new("big_number", DataType::LONG, false),
            StructField::new("pi", DataType::DOUBLE, true),
            StructField::new("flag", DataType::BOOLEAN, false),
        ]));

        let result_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(result_data.as_ref())?;

        assert_eq!(result_batch.num_columns(), 4);
        assert_eq!(result_batch.num_rows(), 2);

        // Check data types
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).data_type(), &ArrowDataType::Int32);
        assert_eq!(schema.field(1).data_type(), &ArrowDataType::Int64);
        assert_eq!(schema.field(2).data_type(), &ArrowDataType::Float64);
        assert_eq!(schema.field(3).data_type(), &ArrowDataType::Boolean);

        Ok(())
    }

    #[test]
    fn test_append_single_column() -> DeltaResult<()> {
        let initial_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let initial_batch = RecordBatch::try_new(
            initial_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("Alice"),
                    Some("Bob"),
                    Some("Charlie"),
                ])),
            ],
        )?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        // Append just one column
        let new_columns = vec![ArrayData::try_new(
            ArrayType::new(DataType::BOOLEAN, false),
            vec![true, false, true],
        )?];

        let new_schema = Arc::new(StructType::new_unchecked([StructField::new(
            "active",
            DataType::BOOLEAN,
            false,
        )]));

        let result_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(result_data.as_ref())?;

        assert_eq!(result_batch.num_columns(), 3);
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.schema().field(2).name(), "active");

        Ok(())
    }

    #[test]
    fn test_binary_column_extraction() -> DeltaResult<()> {
        use crate::arrow::array::BinaryArray;
        use crate::engine_data::{GetData, RowVisitor};
        use crate::schema::ColumnName;
        use std::sync::LazyLock;

        // Create a RecordBatch with binary data
        let binary_data: Vec<Option<&[u8]>> = vec![
            Some(b"hello"),
            Some(b"world"),
            None,
            Some(b"\x00\x01\x02\x03"),
        ];
        let binary_array = BinaryArray::from(binary_data.clone());

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "data",
            ArrowDataType::Binary,
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(binary_array)])?;
        let arrow_data = ArrowEngineData::new(batch);

        // Create a visitor to extract binary data
        struct BinaryVisitor {
            values: Vec<Option<Vec<u8>>>,
        }

        impl RowVisitor for BinaryVisitor {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static NAMES: LazyLock<Vec<ColumnName>> =
                    LazyLock::new(|| vec![ColumnName::new(["data"])]);
                static TYPES: LazyLock<Vec<DataType>> = LazyLock::new(|| vec![DataType::BINARY]);
                (&NAMES, &TYPES)
            }

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                assert_eq!(getters.len(), 1);
                let getter = getters[0];

                for i in 0..row_count {
                    self.values
                        .push(getter.get_binary(i, "data")?.map(|b| b.to_vec()));
                }
                Ok(())
            }
        }

        let mut visitor = BinaryVisitor { values: vec![] };
        arrow_data.visit_rows(&[ColumnName::new(["data"])], &mut visitor)?;

        // Verify the extracted values
        assert_eq!(visitor.values.len(), 4);
        assert_eq!(visitor.values[0].as_deref(), Some(b"hello".as_ref()));
        assert_eq!(visitor.values[1].as_deref(), Some(b"world".as_ref()));
        assert_eq!(visitor.values[2], None);
        assert_eq!(
            visitor.values[3].as_deref(),
            Some(b"\x00\x01\x02\x03".as_ref())
        );

        Ok(())
    }

    #[test]
    fn test_binary_column_extraction_type_mismatch() -> DeltaResult<()> {
        use crate::engine_data::{GetData, RowVisitor};
        use crate::schema::ColumnName;
        use std::sync::LazyLock;

        // Create a RecordBatch with Int32 data (not binary)
        let data: Vec<Option<i32>> = vec![Some(123)];
        let int_array = Int32Array::from(data);

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "data",
            ArrowDataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(int_array)])?;
        let arrow_data = ArrowEngineData::new(batch);

        // Create a visitor that tries to extract binary data from an int column
        struct BinaryVisitor {
            values: Vec<Option<Vec<u8>>>,
        }

        impl RowVisitor for BinaryVisitor {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static NAMES: LazyLock<Vec<ColumnName>> =
                    LazyLock::new(|| vec![ColumnName::new(["data"])]);
                static TYPES: LazyLock<Vec<DataType>> = LazyLock::new(|| vec![DataType::BINARY]);
                (&NAMES, &TYPES)
            }

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                assert_eq!(getters.len(), 1);
                let getter = getters[0];

                for i in 0..row_count {
                    self.values
                        .push(getter.get_binary(i, "data")?.map(|b| b.to_vec()));
                }
                Ok(())
            }
        }

        let mut visitor = BinaryVisitor { values: vec![] };
        let result = arrow_data.visit_rows(&[ColumnName::new(["data"])], &mut visitor);

        // Verify that we get a type mismatch error
        assert_result_error_with_message(
            result,
            "Type mismatch on data: expected binary, got Int32",
        );

        Ok(())
    }

    #[test]
    fn test_struct_extraction_via_get_data() -> DeltaResult<()> {
        use crate::arrow::array::StructArray;
        use crate::engine_data::{GetData, RowVisitor, TypedGetData};
        use crate::expressions::{ColumnName, StructData};
        use std::sync::LazyLock;

        // Create a struct array with nested data
        let inner_fields = vec![
            ArrowField::new("num_records", ArrowDataType::Int64, true),
            ArrowField::new("flag", ArrowDataType::Boolean, true),
        ];
        let inner_struct_type = ArrowDataType::Struct(inner_fields.clone().into());

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("path", ArrowDataType::Utf8, false),
            ArrowField::new("stats", inner_struct_type.clone(), true),
        ]));

        // Create the inner struct array
        let num_records_array = crate::arrow::array::Int64Array::from(vec![
            Some(100i64),
            Some(200i64),
            None, // null value
        ]);
        let flag_array =
            crate::arrow::array::BooleanArray::from(vec![Some(true), Some(false), Some(true)]);

        let stats_struct = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("num_records", ArrowDataType::Int64, true)),
                Arc::new(num_records_array) as crate::arrow::array::ArrayRef,
            ),
            (
                Arc::new(ArrowField::new("flag", ArrowDataType::Boolean, true)),
                Arc::new(flag_array) as crate::arrow::array::ArrayRef,
            ),
        ]);

        let path_array = StringArray::from(vec!["file1.parquet", "file2.parquet", "file3.parquet"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(path_array) as crate::arrow::array::ArrayRef,
                Arc::new(stats_struct) as crate::arrow::array::ArrayRef,
            ],
        )?;

        let arrow_data = ArrowEngineData::new(batch);

        // Create a visitor that extracts struct data
        struct StructVisitor {
            entries: Vec<(String, Option<StructData>)>,
        }

        impl RowVisitor for StructVisitor {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static NAMES: LazyLock<Vec<ColumnName>> =
                    LazyLock::new(|| vec![ColumnName::new(["path"]), ColumnName::new(["stats"])]);
                static TYPES: LazyLock<Vec<DataType>> = LazyLock::new(|| {
                    vec![
                        DataType::STRING,
                        DataType::Struct(Box::new(StructType::new_unchecked(vec![
                            StructField::nullable("num_records", DataType::LONG),
                            StructField::nullable("flag", DataType::BOOLEAN),
                        ]))),
                    ]
                });
                (&NAMES, &TYPES)
            }

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                assert_eq!(getters.len(), 2);
                for i in 0..row_count {
                    let path: String = getters[0].get(i, "path")?;
                    let stats: Option<StructData> = getters[1].get_opt(i, "stats")?;
                    self.entries.push((path, stats));
                }
                Ok(())
            }
        }

        let mut visitor = StructVisitor { entries: vec![] };
        arrow_data.visit_rows(
            &[ColumnName::new(["path"]), ColumnName::new(["stats"])],
            &mut visitor,
        )?;

        // Verify the extracted data
        assert_eq!(visitor.entries.len(), 3);

        // First row: file1.parquet with stats {num_records: 100, flag: true}
        assert_eq!(visitor.entries[0].0, "file1.parquet");
        let stats0 = visitor.entries[0]
            .1
            .as_ref()
            .expect("stats should be present");
        assert_eq!(stats0.fields().len(), 2);
        assert_eq!(stats0.fields()[0].name(), "num_records");
        assert_eq!(stats0.fields()[1].name(), "flag");

        // Second row: file2.parquet with stats {num_records: 200, flag: false}
        assert_eq!(visitor.entries[1].0, "file2.parquet");
        let stats1 = visitor.entries[1]
            .1
            .as_ref()
            .expect("stats should be present");
        assert_eq!(stats1.fields().len(), 2);

        // Third row: file3.parquet with stats {num_records: null, flag: true}
        assert_eq!(visitor.entries[2].0, "file3.parquet");
        let stats2 = visitor.entries[2]
            .1
            .as_ref()
            .expect("stats should be present");
        assert_eq!(stats2.fields().len(), 2);

        Ok(())
    }
}
