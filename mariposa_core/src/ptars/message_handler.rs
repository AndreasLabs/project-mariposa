use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow_array::StructArray;
use protobuf::{MessageDyn, reflect::MessageDescriptor};
use std::sync::Arc;

use crate::ptars::builders::BinaryBuilder;
use crate::ptars::converters::{extract_array, fields_to_arrays};

/// Handler for converting between protobuf messages and Arrow record batches
pub struct MessageHandler {
    message_descriptor: MessageDescriptor,
}

impl MessageHandler {
    /// Create a new MessageHandler for a specific protobuf message type
    pub fn new(message_descriptor: MessageDescriptor) -> Self {
        Self { message_descriptor }
    }
    
    /// Convert a list of serialized protobuf messages to an Arrow RecordBatch
    pub fn list_to_record_batch(&self, values: Vec<Vec<u8>>) -> RecordBatch {
        let messages: Vec<Box<dyn MessageDyn>> = values
            .iter()
            .map(|x| {
                self.message_descriptor
                    .parse_from_bytes(x.as_slice())
                    .unwrap()
            })
            .collect();

        let arrays = fields_to_arrays(&messages, &self.message_descriptor);
        
        // Create a struct array from the fields and arrays
        let struct_array = if arrays.is_empty() {
            StructArray::new_empty_fields(messages.len(), None)
        } else {
            StructArray::from(arrays)
        };
        
        RecordBatch::from(struct_array)
    }
    
    /// Convert a record batch back to serialized protobuf messages
    pub fn record_batch_to_array(&self, record_batch: &RecordBatch) -> Vec<Vec<u8>> {
        // Create new message instances for each row
        let mut messages: Vec<Box<dyn MessageDyn>> = (0..record_batch.num_rows())
            .map(|_| self.message_descriptor.new_instance())
            .collect();
        
        // Extract data from record batch into messages
        self.message_descriptor
            .fields()
            .for_each(|field_descriptor| {
                let column: Option<&ArrayRef> = record_batch.column_by_name(field_descriptor.name());
                if let Some(column) = column {
                    extract_array(column, &field_descriptor, &mut messages);
                }
            });
        
        // Serialize messages to byte arrays
        let mut results = Vec::with_capacity(messages.len());
        for message in messages {
            results.push(message.write_to_bytes_dyn().unwrap());
        }
        
        results
    }
    
    /// Get the underlying message descriptor
    pub fn get_message_descriptor(&self) -> &MessageDescriptor {
        &self.message_descriptor
    }
} 