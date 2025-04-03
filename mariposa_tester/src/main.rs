use arrow::array::{Float32Array, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::print_batches;
use colored::*;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, ReflectMessage, Value};
use std::sync::Arc;
use std::time::Instant;

// Import the protobuf types generated from tester.proto
use mariposa_tester::tester::tester::{TestPoseMessage, Vector3, Quaternion};
use mariposa_tester::create_test_pose;

fn main() {
    println!("{}", "=== Mariposa Tester ===".green().bold());
    println!("{}", "Demonstrating Protobuf with prost and prost-reflect".blue());
    println!();

    // Create a test message
    let test_pose = create_test_pose();
    
    // Display the message
    println!("{}", "Original TestPoseMessage:".yellow());
    print_test_pose(&test_pose);
    println!();
    
    // -----------------
    // Prost serialization
    // -----------------
    println!("{}", "1. Prost serialization:".cyan().bold());
    
    // Time the serialization
    let start = Instant::now();
    let bytes = test_pose.encode_to_vec();
    let duration = start.elapsed();
    
    println!("Serialized in: {:?}", duration);
    println!("Serialized size: {} bytes", bytes.len());
    println!("First 20 bytes: {:?}", &bytes[..20.min(bytes.len())]);
    println!();
    
    // -----------------
    // Prost-reflect demonstration
    // -----------------
    println!("{}", "2. Prost-reflect demonstration:".cyan().bold());
    
    // Get the file descriptor for the proto message
    let file_descriptor_bytes = match std::fs::read(format!("{}/tester.bin", std::env::var("OUT_DIR").unwrap_or_else(|_| "../target/debug/build/mariposa_tester-*/out".to_string()))) {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("Error reading file descriptor: {}", e);
            println!("Will try to continue without prost-reflect functionality");
            vec![]
        }
    };

    if !file_descriptor_bytes.is_empty() {
        match DescriptorPool::decode(&file_descriptor_bytes[..]) {
            Ok(pool) => {
                match pool.get_message_by_name("tester.TestPoseMessage") {
                    Some(message_descriptor) => {
                        println!("Found message descriptor for TestPoseMessage!");
                        println!("Fields:");
                        for field in message_descriptor.fields() {
                            println!("  - {}: {:?} (tag: {})", field.name(), field.kind(), field.number());
                        }
                        
                        // Deserialize using prost-reflect for dynamic access
                        match DynamicMessage::decode(message_descriptor.clone(), &bytes[..]) {
                            Ok(dynamic_message) => {
                                println!("\nDynamic message access:");
                                if let Some(position_field) = dynamic_message.get_field_by_name("position") {
                                    if let Value::Message(position_msg) = position_field.as_ref() {
                                        println!("  Position.x: {}", 
                                            position_msg.get_field_by_name("x")
                                                .and_then(|v| v.as_ref().as_f32())
                                                .unwrap_or(0.0));
                                    }
                                }
                                println!("\nAll field names in the message:");
                                for field in message_descriptor.fields() {
                                    println!("  - {}", field.name());
                                }
                            },
                            Err(e) => println!("Failed to decode dynamic message: {}", e),
                        }
                    },
                    None => println!("Could not find message descriptor for TestPoseMessage"),
                }
            },
            Err(e) => println!("Failed to decode descriptor pool: {}", e),
        }
    }
    
    println!();
    
    // -----------------
    // Manual conversion to Arrow format
    // -----------------
    println!("{}", "3. Manual conversion to Arrow format:".cyan().bold());
    
    // Create a simple Arrow RecordBatch from our TestPoseMessage
    let record_batch = create_arrow_record_batch(&test_pose);
    
    // Print the record batch
    println!("Arrow RecordBatch Schema:");
    println!("{}", record_batch.schema().to_string());
    println!("\nArrow RecordBatch Data:");
    print_batches(&[record_batch]).unwrap();
}

fn print_test_pose(pose: &TestPoseMessage) {
    if let Some(position) = &pose.position {
        println!("  Position: ({}, {}, {})", position.x, position.y, position.z);
    }
    if let Some(velocity) = &pose.velocity {
        println!("  Velocity: ({}, {}, {})", velocity.x, velocity.y, velocity.z);
    }
    if let Some(acceleration) = &pose.acceleration {
        println!("  Acceleration: ({}, {}, {})", acceleration.x, acceleration.y, acceleration.z);
    }
    if let Some(orientation) = &pose.orientation {
        println!("  Orientation: ({}, {}, {}, {})", orientation.x, orientation.y, orientation.z, orientation.w);
    }
    if let Some(angular_velocity) = &pose.angular_velocity {
        println!("  Angular Velocity: ({}, {}, {})", angular_velocity.x, angular_velocity.y, angular_velocity.z);
    }
    if let Some(angular_acceleration) = &pose.angular_acceleration {
        println!("  Angular Acceleration: ({}, {}, {})", angular_acceleration.x, angular_acceleration.y, angular_acceleration.z);
    }
}

/// Manual conversion of TestPoseMessage to an Arrow RecordBatch
fn create_arrow_record_batch(pose: &TestPoseMessage) -> RecordBatch {
    // Create schema for our record batch
    let schema = Schema::new(vec![
        Field::new("component", DataType::Utf8, false),
        Field::new("x", DataType::Float32, true),
        Field::new("y", DataType::Float32, true),
        Field::new("z", DataType::Float32, true),
        Field::new("w", DataType::Float32, true),
    ]);
    
    // Create arrays for each column
    let mut component_values = Vec::new();
    let mut x_values = Vec::new();
    let mut y_values = Vec::new();
    let mut z_values = Vec::new();
    let mut w_values = Vec::new();
    
    // Add position data
    if let Some(position) = &pose.position {
        component_values.push("position");
        x_values.push(Some(position.x));
        y_values.push(Some(position.y));
        z_values.push(Some(position.z));
        w_values.push(None);
    }
    
    // Add velocity data
    if let Some(velocity) = &pose.velocity {
        component_values.push("velocity");
        x_values.push(Some(velocity.x));
        y_values.push(Some(velocity.y));
        z_values.push(Some(velocity.z));
        w_values.push(None);
    }
    
    // Add acceleration data
    if let Some(acceleration) = &pose.acceleration {
        component_values.push("acceleration");
        x_values.push(Some(acceleration.x));
        y_values.push(Some(acceleration.y));
        z_values.push(Some(acceleration.z));
        w_values.push(None);
    }
    
    // Add orientation data
    if let Some(orientation) = &pose.orientation {
        component_values.push("orientation");
        x_values.push(Some(orientation.x));
        y_values.push(Some(orientation.y));
        z_values.push(Some(orientation.z));
        w_values.push(Some(orientation.w));
    }
    
    // Add angular velocity data
    if let Some(angular_velocity) = &pose.angular_velocity {
        component_values.push("angular_velocity");
        x_values.push(Some(angular_velocity.x));
        y_values.push(Some(angular_velocity.y));
        z_values.push(Some(angular_velocity.z));
        w_values.push(None);
    }
    
    // Add angular acceleration data
    if let Some(angular_acceleration) = &pose.angular_acceleration {
        component_values.push("angular_acceleration");
        x_values.push(Some(angular_acceleration.x));
        y_values.push(Some(angular_acceleration.y));
        z_values.push(Some(angular_acceleration.z));
        w_values.push(None);
    }
    
    // Create the arrays
    let component_array = Arc::new(StringArray::from(component_values));
    let x_array = Arc::new(Float32Array::from(x_values));
    let y_array = Arc::new(Float32Array::from(y_values));
    let z_array = Arc::new(Float32Array::from(z_values));
    let w_array = Arc::new(Float32Array::from(w_values));
    
    // Create the record batch
    RecordBatch::try_new(
        Arc::new(schema),
        vec![component_array, x_array, y_array, z_array, w_array],
    ).unwrap()
} 