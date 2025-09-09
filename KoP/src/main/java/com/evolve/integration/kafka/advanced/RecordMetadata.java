package com.evolve.integration.kafka.advanced;

import org.apache.pulsar.client.api.MessageId;

public class RecordMetadata {
    public final String topic; public final MessageId messageId; public final long timestamp;
    RecordMetadata(String topic, MessageId id, long ts) { this.topic = topic; this.messageId = id; this.timestamp = ts; }
}
