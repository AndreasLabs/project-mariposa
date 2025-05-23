# Raw Notes
- Pub/Sub simplistic robotics IPC system similar to ROS 1.
- Use Apache Arrow for core primitives and data-storage. 
- `RecordBatches` can show columns or our struct where each row represents a time entry.
- `RecordBatches` can store meta data such as topic and schema.
- `RecordBatches` can be concat'd, streamed , chunked and stored.
- Logging can be done by writing to a file version of record batches.
- Instead of storing a store-per-topic, instead we can simply do a more pure "sub pub" approach, "streaming" data directly at the time of reception to each client.
	- Can either do this direct 1-1 or each client/sub can store a record batch of queued updates and use this. May involve a copy tho? 
	- Rust channels into each connection can provide the queue support also.
	- The "Sender" thread for these can be rate limited and just dump the queue into a record batch for sending. 
- Actually we should split up based on clients, clients will have many subs and that may be too much. Instead we can check of a given client has subs that want this message. Then send it to the client. 
	- To still support individual per-topic handlers, we can include a vector of "subscription id" that the given record batch should notify. The client can use this to notify all the subscriptions with only the single sent topic.
	- Can maybe do this automatically on the client by running our topic query check and figure out at runtime which handlers to notify.