// Protobuf to Arrow and Arrow to Protobuf conversion

mod message_handler;
mod proto_cache;
mod converters;
mod builders;
mod examples;

#[cfg(test)]
mod tests;

pub use message_handler::MessageHandler;
pub use proto_cache::ProtoCache;
pub use examples::usage_example;

// Constants
static CE_OFFSET: i32 = 719163; // Offset for date conversion 