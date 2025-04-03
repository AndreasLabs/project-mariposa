use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    
    // Configure and compile the protobuf files
    let mut config = prost_build::Config::new();
    
    // Save the file descriptor set for use with prost-reflect
    config.file_descriptor_set_path(out_dir.join("tester.bin"));
    
    // Compile the proto files
    config.compile_protos(&["tester_proto/tester.proto"], &["tester_proto/"])?;
    
    Ok(())
}