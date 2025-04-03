/// Example of how to use the ptars module

#[cfg(test)]
mod examples {
    use arrow::record_batch::RecordBatch;
    use arrow_array::{Int32Array, Float32Array};
    use std::sync::Arc;

    /// Example creating a simple Arrow RecordBatch
    pub fn create_example_table() -> RecordBatch {
        let col_1 = Arc::new(Int32Array::from_iter([1, 2, 3])) as _;
        let col_2 = Arc::new(Float32Array::from_iter([1.0, 6.3, 4.0])) as _;
        
        RecordBatch::try_from_iter([("col1", col_1), ("col_2", col_2)]).unwrap()
    }
}

/// Example of ptars usage in a normal module
pub mod usage_example {
    use crate::ptars::{MessageHandler, ProtoCache};
    use arrow::record_batch::RecordBatch;
    
    /// Process a protobuf message file to an Arrow RecordBatch
    pub fn process_proto_to_arrow(
        proto_bytes: Vec<Vec<u8>>,
        message_name: &str,
        descriptors: Vec<Vec<u8>>,
    ) -> RecordBatch {
        // Create a cache for protobuf descriptors
        let mut cache = ProtoCache::new();
        
        // Create a message handler for the specified message type
        let handler = cache.create_for_message(message_name.to_string(), descriptors);
        
        // Convert the protobuf messages to an Arrow record batch
        handler.list_to_record_batch(proto_bytes)
    }
    
    /// Process an Arrow RecordBatch back to protobuf messages
    pub fn process_arrow_to_proto(
        record_batch: &RecordBatch,
        message_name: &str,
        descriptors: Vec<Vec<u8>>,
    ) -> Vec<Vec<u8>> {
        // Create a cache for protobuf descriptors
        let mut cache = ProtoCache::new();
        
        // Create a message handler for the specified message type
        let handler = cache.create_for_message(message_name.to_string(), descriptors);
        
        // Convert the Arrow record batch back to protobuf messages
        handler.record_batch_to_array(record_batch)
    }
} 