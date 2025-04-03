pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

// Protobuf to Arrow and Arrow to Protobuf conversion
pub mod ptars;

/// Example of using the ptars module to convert between protobuf and arrow
/// 
/// ```rust,ignore
/// use mariposa_core::ptars::{ProtoCache, usage_example};
/// 
/// // Get protobuf file descriptors and message data
/// let descriptors = get_descriptors();
/// let proto_messages = get_proto_messages();
/// let message_name = "example.MyMessage";
/// 
/// // Convert protobuf to arrow
/// let record_batch = usage_example::process_proto_to_arrow(
///     proto_messages,
///     message_name,
///     descriptors.clone()
/// );
/// 
/// // Work with the Arrow record batch...
/// 
/// // Convert back to protobuf
/// let proto_messages_out = usage_example::process_arrow_to_proto(
///     &record_batch,
///     message_name,
///     descriptors
/// );
/// ```
///
/// where `get_descriptors()` and `get_proto_messages()` are your functions
/// to retrieve the protobuf file descriptors and serialized message data.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
