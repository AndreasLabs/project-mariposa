# PTars - Protobuf to Arrow Rust library

PTars (Protobuf to Arrow) is a Rust library for converting between Protocol Buffers (protobuf) messages and Apache Arrow data structures. This enables efficient data interchange between systems using protobuf for messaging and Arrow for analytics.

## Features

- Convert serialized protobuf messages to Arrow RecordBatch
- Convert Arrow RecordBatch back to serialized protobuf messages
- Support for all basic protobuf types (i32, i64, f32, f64, bool, string, bytes)
- Support for nested message types
- Date and timestamp conversion utilities
- Caching of file descriptors for better performance

## Usage

```rust
use mariposa_core::ptars::{ProtoCache, MessageHandler};

// Create a cache for protobuf file descriptors
let mut cache = ProtoCache::new();

// Get file descriptor protos (usually from a .proto file)
let descriptors: Vec<Vec<u8>> = get_descriptors();
let message_name = "example.MyMessage";

// Create a message handler for the specific message type
let handler = cache.create_for_message(message_name.to_string(), descriptors);

// Convert serialized protobuf messages to Arrow RecordBatch
let proto_messages: Vec<Vec<u8>> = get_proto_messages();
let record_batch = handler.list_to_record_batch(proto_messages);

// Process the Arrow RecordBatch...

// Convert back to protobuf messages
let proto_messages_out = handler.record_batch_to_array(&record_batch);
```

## Implementation

This library is a pure Rust implementation based on the `ptars` Python/Rust library by 0x26res. It uses `protobuf` for Protocol Buffer handling and `arrow` for Apache Arrow integration.

## Limitations

- Limited support for complex nested types
- No direct support for repeated fields yet (needs to be added)
- No direct support for map fields yet (needs to be added) 