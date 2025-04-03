use arrow::array::ArrayRef;
use arrow_array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array, PrimitiveArray,
    TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow_array::builder::Int32Builder;
use arrow_schema::DataType;
use chrono::Datelike;
use protobuf::reflect::{FieldDescriptor, MessageDescriptor, ReflectValueRef, RuntimeType};
use protobuf::MessageDyn;
use std::iter::zip;
use std::ops::Deref;
use std::sync::Arc;

use crate::ptars::CE_OFFSET;
use crate::ptars::builders::{StringBuilder, BinaryBuilder};

/// Reads primitive values from protobuf messages into Arrow arrays
pub fn read_primitive<T, A>(
    messages: &Vec<Box<dyn MessageDyn>>,
    field: &FieldDescriptor,
    extract_fn: &dyn Fn(ReflectValueRef) -> T,
    default_value: T,
) -> Arc<A>
where
    T: Clone,
    A: From<Vec<T>> + Array,
{
    let mut values: Vec<T> = Vec::with_capacity(messages.len());
    for message in messages {
        let value = match field.get_singular(message.as_ref()) {
            None => default_value.clone(),
            Some(x) => extract_fn(x),
        };
        values.push(value);
    }
    Arc::new(A::from(values))
}

/// Converts a nested message field into an Arrow array
pub fn nested_messages_to_array(
    field: &FieldDescriptor,
    message_descriptor: MessageDescriptor,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> ArrayRef {
    let mut builder = BinaryBuilder::new();
    
    for message in messages {
        let nested_message = field.get_singular(message.as_ref());
        match nested_message {
            None => builder.offsets.push(i32::try_from(builder.values.len()).unwrap()),
            Some(x) => {
                if let Ok(reflect_message) = x.to_message() {
                    builder.append_message(reflect_message);
                } else {
                    builder.offsets.push(i32::try_from(builder.values.len()).unwrap());
                }
            }
        }
    }
    
    builder.build()
}

/// Helper function to read i32 values from protobuf messages
pub fn read_i32(message: &dyn MessageDyn, field_descriptor: &FieldDescriptor) -> i32 {
    return match field_descriptor.get_singular(message) {
        None => 0,
        Some(x) => x.to_i32().unwrap(),
    };
}

/// Converts protobuf date messages to Arrow Date32Array
pub fn convert_date(
    messages: &Vec<Box<dyn MessageDyn>>,
    is_valid: &Vec<bool>,
    message_descriptor: &MessageDescriptor,
) -> Arc<Date32Array> {
    let year_descriptor = message_descriptor.field_by_name("year").unwrap();
    let month_descriptor = message_descriptor.field_by_name("month").unwrap();
    let day_descriptor = message_descriptor.field_by_name("day").unwrap();
    
    let mut builder = Int32Builder::new();
    
    for (message, message_valid) in zip(messages, is_valid) {
        if *message_valid {
            let year: i32 = read_i32(message.deref(), &year_descriptor);
            let month: i32 = read_i32(message.deref(), &month_descriptor);
            let day: i32 = read_i32(message.deref(), &day_descriptor);
            
            if (year == 0) && (month == 0) && (day == 0) {
                builder.append_value(0)
            } else {
                builder.append_value(
                    chrono::NaiveDate::from_ymd_opt(
                        year,
                        u32::try_from(month).unwrap(),
                        u32::try_from(day).unwrap(),
                    )
                    .unwrap()
                    .num_days_from_ce() - CE_OFFSET,
                )
            }
        } else {
            builder.append_null()
        }
    }
    
    Arc::new(builder.finish().reinterpret_cast())
}

/// Converts protobuf timestamp messages to Arrow TimestampNanosecondArray
pub fn convert_timestamps(
    arrays: &Vec<(Arc<arrow_schema::Field>, ArrayRef)>,
    is_valid: &Vec<bool>,
) -> Arc<TimestampNanosecondArray> {
    if arrays.is_empty() {
        return Arc::new(TimestampNanosecondArray::from(Vec::<i64>::new()));
    }
    
    let seconds_array = arrays
        .iter()
        .find(|(field, _)| field.name() == "seconds")
        .map(|(_, array)| array.as_primitive::<Int64Array>())
        .unwrap();
    
    let nanos_array = arrays
        .iter()
        .find(|(field, _)| field.name() == "nanos")
        .map(|(_, array)| array.as_primitive::<Int32Array>())
        .unwrap();
    
    let mut results: Vec<i64> = vec![];
    
    for i in 0..seconds_array.len() {
        let seconds = seconds_array.value(i);
        let nanos = if i < nanos_array.len() {
            nanos_array.value(i)
        } else {
            0
        };
        results.push(seconds * 1_000_000_000i64 + nanos as i64);
    }
    
    let array = TimestampNanosecondArray::from(results);
    
    if is_valid.is_empty() {
        return Arc::new(array);
    }
    
    let mask = BooleanArray::from(
        is_valid
            .iter()
            .map(|x| !x)
            .take(array.len())
            .collect::<Vec<bool>>(),
    );
    
    let nullified = arrow::compute::nullif(&array, &mask).unwrap();
    Arc::new(nullified.into_array().as_primitive::<TimestampNanosecondArray>().clone())
}

/// Extract singular field values from protobuf messages into Arrow arrays
pub fn singular_field_to_array(
    field: &FieldDescriptor,
    runtime_type: &RuntimeType,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Result<ArrayRef, &'static str> {
    match runtime_type {
        RuntimeType::I32 => Ok(read_primitive::<i32, Int32Array>(
            messages,
            field,
            &ReflectValueRef::to_i32,
            0,
        )),
        RuntimeType::U32 => Ok(read_primitive::<u32, UInt32Array>(
            messages,
            field,
            &ReflectValueRef::to_u32,
            0,
        )),
        RuntimeType::I64 => Ok(read_primitive::<i64, Int64Array>(
            messages,
            field,
            &ReflectValueRef::to_i64,
            0,
        )),
        RuntimeType::U64 => Ok(read_primitive::<u64, UInt64Array>(
            messages,
            field,
            &ReflectValueRef::to_u64,
            0,
        )),
        RuntimeType::F32 => Ok(read_primitive::<f32, Float32Array>(
            messages,
            field,
            &ReflectValueRef::to_f32,
            0.0,
        )),
        RuntimeType::F64 => Ok(read_primitive::<f64, Float64Array>(
            messages,
            field,
            &ReflectValueRef::to_f64,
            0.0,
        )),
        RuntimeType::Bool => Ok(read_primitive::<bool, BooleanArray>(
            messages,
            field,
            &ReflectValueRef::to_bool,
            false,
        )),
        RuntimeType::String => {
            let mut builder = StringBuilder::new();
            for message in messages {
                builder.append(message.as_ref(), field)
            }
            Ok(builder.build())
        }
        RuntimeType::VecU8 => {
            let mut builder = BinaryBuilder::new();
            for message in messages {
                builder.append(message.as_ref(), field);
            }
            Ok(builder.build())
        }
        RuntimeType::Enum(_) => Ok(read_primitive::<i32, Int32Array>(
            messages,
            field,
            &ReflectValueRef::to_enum_value,
            0,
        )),
        RuntimeType::Message(x) => Ok(nested_messages_to_array(field, x, messages)),
    }
}

/// Convert fields from protobuf messages to (field, array) pairs for Arrow
pub fn fields_to_arrays(messages: &Vec<Box<dyn MessageDyn>>, message_descriptor: &MessageDescriptor) 
    -> Vec<(Arc<arrow_schema::Field>, ArrayRef)> 
{
    let mut result = Vec::new();
    
    for field_descriptor in message_descriptor.fields() {
        match field_descriptor.runtime_field_type() {
            protobuf::reflect::RuntimeFieldType::Singular(x) => {
                let field_arrow_type = match x {
                    RuntimeType::I32 => DataType::Int32,
                    RuntimeType::U32 => DataType::UInt32,
                    RuntimeType::I64 => DataType::Int64,
                    RuntimeType::U64 => DataType::UInt64,
                    RuntimeType::F32 => DataType::Float32,
                    RuntimeType::F64 => DataType::Float64,
                    RuntimeType::Bool => DataType::Boolean,
                    RuntimeType::String => DataType::Utf8,
                    RuntimeType::VecU8 => DataType::Binary,
                    RuntimeType::Enum(_) => DataType::Int32,
                    RuntimeType::Message(_) => DataType::Binary,
                };
                
                let array_result = singular_field_to_array(&field_descriptor, &x, messages);
                if let Ok(array) = array_result {
                    let field = Arc::new(arrow_schema::Field::new(
                        field_descriptor.name(),
                        field_arrow_type,
                        true,
                    ));
                    result.push((field, array));
                }
            }
            _ => {} // Skip repeated and map fields for now
        }
    }
    
    result
}

/// Extract values from an Arrow array into protobuf messages
pub fn extract_singular_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [Box<dyn MessageDyn>],
    runtime_type: &RuntimeType,
) {
    match runtime_type {
        RuntimeType::I32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut dyn MessageDyn = messages.get_mut(index).unwrap().as_mut();
                    field_descriptor.set_singular_field(&mut *element, protobuf::reflect::ReflectValueBox::I32(x));
                }
            }),
        RuntimeType::U32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut dyn MessageDyn = messages.get_mut(index).unwrap().as_mut();
                    field_descriptor.set_singular_field(&mut *element, protobuf::reflect::ReflectValueBox::U32(x));
                }
            }),
        RuntimeType::I64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut dyn MessageDyn = messages.get_mut(index).unwrap().as_mut();
                    field_descriptor.set_singular_field(&mut *element, protobuf::reflect::ReflectValueBox::I64(x));
                }
            }),
        RuntimeType::U64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut dyn MessageDyn = messages.get_mut(index).unwrap().as_mut();
                    field_descriptor.set_singular_field(&mut *element, protobuf::reflect::ReflectValueBox::U64(x));
                }
            }),
        RuntimeType::F32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut dyn MessageDyn = messages.get_mut(index).unwrap().as_mut();
                    field_descriptor.set_singular_field(&mut *element, protobuf::reflect::ReflectValueBox::F32(x));
                }
            }),
        RuntimeType::F64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut dyn MessageDyn = messages.get_mut(index).unwrap().as_mut();
                    field_descriptor.set_singular_field(&mut *element, protobuf::reflect::ReflectValueBox::F64(x));
                }
            }),
        RuntimeType::Bool => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut dyn MessageDyn = messages.get_mut(index).unwrap().as_mut();
                    field_descriptor.set_singular_field(&mut *element, protobuf::reflect::ReflectValueBox::Bool(x));
                }
            }),
        RuntimeType::String => array
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut dyn MessageDyn = messages.get_mut(index).unwrap().as_mut();
                    field_descriptor.set_singular_field(&mut *element, protobuf::reflect::ReflectValueBox::String(x.to_string()));
                }
            }),
        RuntimeType::VecU8 => array
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut dyn MessageDyn = messages.get_mut(index).unwrap().as_mut();
                    field_descriptor.set_singular_field(&mut *element, protobuf::reflect::ReflectValueBox::Bytes(x.to_vec()));
                }
            }),
        RuntimeType::Enum(_) => {} // Not implemented yet
        RuntimeType::Message(_) => {} // Not implemented yet
    }
}

/// Extract an Arrow array into protobuf messages
pub fn extract_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [Box<dyn MessageDyn>],
) {
    match field_descriptor.runtime_field_type() {
        protobuf::reflect::RuntimeFieldType::Singular(x) => {
            extract_singular_array(array, field_descriptor, messages, &x)
        }
        protobuf::reflect::RuntimeFieldType::Repeated(_) => {} // Not implemented yet
        protobuf::reflect::RuntimeFieldType::Map(_, _) => {} // Not implemented yet
    }
} 