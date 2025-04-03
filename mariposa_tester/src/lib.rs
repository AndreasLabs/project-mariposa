pub mod tester {
    pub mod tester {
        include!(concat!(env!("OUT_DIR"), "/tester.rs"));
    }
}

use prost::Message;
use tester::tester::{TestPoseMessage, Vector3, Quaternion};

/// Create a test instance of TestPoseMessage
pub fn create_test_pose() -> TestPoseMessage {
    TestPoseMessage {
        position: Some(Vector3 {
            x: 1.0,
            y: 2.0,
            z: 3.0,
        }),
        velocity: Some(Vector3 {
            x: 4.0,
            y: 5.0,
            z: 6.0,
        }),
        acceleration: Some(Vector3 {
            x: 7.0,
            y: 8.0,
            z: 9.0,
        }),
        orientation: Some(Quaternion {
            x: 0.0,
            y: 0.0,
            z: 0.0,
            w: 1.0,
        }),
        angular_velocity: Some(Vector3 {
            x: 0.1,
            y: 0.2,
            z: 0.3,
        }),
        angular_acceleration: Some(Vector3 {
            x: 0.4,
            y: 0.5,
            z: 0.6,
        }),
    }
}

/// Serialize a TestPoseMessage to bytes and print its structure
pub fn test_proto_serialization() {
    // Create a test message
    let test_pose = create_test_pose();
    
    // Print the message structure
    println!("TestPoseMessage structure:");
    println!("  Position: ({}, {}, {})", 
        test_pose.position.as_ref().unwrap().x,
        test_pose.position.as_ref().unwrap().y,
        test_pose.position.as_ref().unwrap().z
    );
    println!("  Velocity: ({}, {}, {})", 
        test_pose.velocity.as_ref().unwrap().x,
        test_pose.velocity.as_ref().unwrap().y,
        test_pose.velocity.as_ref().unwrap().z
    );
    
    // Serialize the message to bytes
    let bytes = test_pose.encode_to_vec();
    println!("\nSerialized data (first 20 bytes): {:?}", &bytes[..20.min(bytes.len())]);
    println!("Total serialized size: {} bytes", bytes.len());
    
    // Deserialize back to validate
    let decoded = TestPoseMessage::decode(bytes.as_slice()).unwrap();
    println!("\nDeserialized position: ({}, {}, {})",
        decoded.position.as_ref().unwrap().x,
        decoded.position.as_ref().unwrap().y,
        decoded.position.as_ref().unwrap().z
    );
}

/// This is an example of how to convert a protobuf message to an Arrow RecordBatch
/// using the ptars library when it becomes functional.
/// 
/// Note: This function is not functional and is provided as a guide.
/// 
/// ```no_run
/// // When ptars is fixed, this is how it would be used:
/// fn proto_to_arrow_example() {
///     use arrow::util::pretty::print_batches;
///     use mariposa_core::ptars::{MessageHandler, ProtoCache};
/// 
///     // Create a test message
///     let test_pose = create_test_pose();
///     
///     // Serialize the message to bytes
///     let proto_bytes = test_pose.encode_to_vec();
///     
///     // Get the file descriptor
///     // In a real application, this would be generated during build
///     let descriptor_bytes = include_bytes!(concat!(env!("OUT_DIR"), "/tester.bin"));
///     
///     // Create a cache for protobuf descriptors
///     let mut cache = ProtoCache::new();
///     
///     // Create a message handler for TestPoseMessage
///     let handler = cache.create_for_message(
///         "tester.TestPoseMessage".to_string(),
///         vec![descriptor_bytes.to_vec()],
///     );
///     
///     // Convert the protobuf message to an Arrow record batch
///     let record_batch = handler.list_to_record_batch(vec![proto_bytes]);
///     
///     // Print the record batch
///     print_batches(&[record_batch]).unwrap();
///     
///     // The Arrow RecordBatch would contain columns for each field in the protobuf message:
///     // - position (struct with x, y, z)
///     // - velocity (struct with x, y, z)
///     // - acceleration (struct with x, y, z)
///     // - orientation (struct with x, y, z, w)
///     // - angular_velocity (struct with x, y, z)
///     // - angular_acceleration (struct with x, y, z)
/// }
/// ```

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_serialization() {
        test_proto_serialization();
    }
}

