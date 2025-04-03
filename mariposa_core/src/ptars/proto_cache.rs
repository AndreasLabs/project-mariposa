use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use std::collections::HashMap;

use crate::ptars::message_handler::MessageHandler;

/// Cache for protobuf file descriptors to avoid repeated parsing
pub struct ProtoCache {
    cache: HashMap<String, FileDescriptor>,
}

impl ProtoCache {
    /// Create a new empty ProtoCache
    pub fn new() -> Self {
        ProtoCache {
            cache: HashMap::new(),
        }
    }
    
    /// Get or create a file descriptor from a proto file, caching the result
    fn get_or_create(&mut self, file_descriptor_proto: &FileDescriptorProto) -> FileDescriptor {
        let name = file_descriptor_proto.name.as_ref().unwrap();
        
        // Check if the descriptor is already in the cache
        if let Some(descriptor) = self.cache.get(name.as_str()) {
            return descriptor.clone();
        }
        
        // Load dependencies first
        let dependencies: Vec<FileDescriptor> = file_descriptor_proto
            .dependency
            .iter()
            .map(|x| self.cache.get(x.as_str()).unwrap().clone())
            .collect();
            
        // Create the new file descriptor
        let descriptor = FileDescriptor::new_dynamic(
            file_descriptor_proto.clone(), 
            &dependencies
        ).unwrap();
        
        // Cache the new descriptor
        self.cache.insert(name.to_string(), descriptor.clone());
        descriptor
    }
    
    /// Create a MessageHandler for a specific message type
    pub fn create_for_message(
        &mut self,
        message_name: String,
        file_descriptors_bytes: Vec<Vec<u8>>,
    ) -> MessageHandler {
        // Parse file descriptor protos
        let file_descriptors_protos: Vec<FileDescriptorProto> = file_descriptors_bytes
            .iter()
            .map(|x| FileDescriptorProto::parse_from_bytes(x.as_slice()).unwrap())
            .collect();
        
        // Build file descriptors in reverse order (dependencies first)
        let file_descriptors: Vec<FileDescriptor> = file_descriptors_protos
            .iter()
            .rev()
            .map(|x| self.get_or_create(x))
            .collect();
        
        // Find the message descriptor by name
        let message_descriptor: MessageDescriptor = file_descriptors
            .last()
            .unwrap()
            .message_by_full_name(message_name.as_str())
            .unwrap();
            
        MessageHandler::new(message_descriptor)
    }
} 