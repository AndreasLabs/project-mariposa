use arrow::array::ArrayData;
use arrow::buffer::Buffer;
use arrow::datatypes::ToByteSlice;
use arrow_array::{BinaryArray, StringArray};
use protobuf::{MessageDyn, reflect::{FieldDescriptor, ReflectValueRef}};
use std::sync::Arc;

/// Builder for constructing Arrow StringArray from Protobuf fields
pub struct StringBuilder {
    values: String,
    offsets: Vec<i32>,
}

impl StringBuilder {
    pub fn new() -> Self {
        Self {
            values: String::new(),
            offsets: Vec::new(),
        }
    }

    /// Append a singular value from a protobuf message field
    pub fn append(&mut self, message: &dyn MessageDyn, field: &FieldDescriptor) {
        self.offsets.push(i32::try_from(self.values.len()).unwrap());
        match field.get_singular(message) {
            None => {}
            Some(x) => self.values.push_str(x.to_str().unwrap()),
        }
    }

    /// Append a value from a ReflectValueRef
    pub fn append_ref(&mut self, reflect_value_ref: ReflectValueRef) {
        self.offsets.push(i32::try_from(self.values.len()).unwrap());
        self.values.push_str(reflect_value_ref.to_str().unwrap())
    }

    /// Returns the current number of elements
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Build the StringArray from collected values
    pub fn build(&mut self) -> Arc<StringArray> {
        let size = self.offsets.len();
        self.offsets.push(i32::try_from(self.values.len()).unwrap());
        let array_data = ArrayData::builder(arrow::datatypes::DataType::Utf8)
            .len(size)
            .add_buffer(Buffer::from_vec(self.offsets.to_vec()))
            .add_buffer(Buffer::from(self.values.as_bytes()))
            .build()
            .unwrap();
        Arc::new(StringArray::from(array_data))
    }
}

/// Builder for constructing Arrow BinaryArray from Protobuf fields
pub struct BinaryBuilder {
    values: Vec<u8>,
    offsets: Vec<i32>,
}

impl BinaryBuilder {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Append a singular value from a protobuf message field
    pub fn append(&mut self, message: &dyn MessageDyn, field: &FieldDescriptor) {
        self.offsets.push(i32::try_from(self.values.len()).unwrap());
        match field.get_singular(message) {
            None => {}
            Some(x) => {
                for c in x.to_bytes().unwrap() {
                    self.values.push(*c)
                }
            }
        }
    }

    /// Append a value from a ReflectValueRef
    pub fn append_ref(&mut self, reflect_value_ref: ReflectValueRef) {
        self.offsets.push(i32::try_from(self.values.len()).unwrap());
        for c in reflect_value_ref.to_bytes().unwrap() {
            self.values.push(*c)
        }
    }

    /// Append an entire message as serialized bytes
    pub fn append_message(&mut self, message: &dyn MessageDyn) {
        let bytes = message.write_to_bytes_dyn().unwrap();
        let offset = i32::try_from(self.values.len()).unwrap();
        self.offsets.push(offset);
        self.values.extend(bytes);
    }

    /// Returns the current number of elements
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Build the BinaryArray from collected values
    pub fn build(&mut self) -> Arc<BinaryArray> {
        let size = self.offsets.len();
        self.offsets.push(i32::try_from(self.values.len()).unwrap());
        let array_data = ArrayData::builder(arrow::datatypes::DataType::Binary)
            .len(size)
            .add_buffer(Buffer::from(self.offsets.to_byte_slice()))
            .add_buffer(Buffer::from_iter(self.values.clone()))
            .build()
            .unwrap();
        Arc::new(BinaryArray::from(array_data))
    }
} 