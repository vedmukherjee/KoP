package com.evolve.integration.kafka.advanced;

import java.util.Map;

import org.apache.pulsar.client.api.MessageId;

public class ConsumerRecord<K,V> {
    public final String topic; public final String key; public final K k; public final V v; public final MessageId messageId; public final long publishTime;
    public final Map<String, String> properties;
    ConsumerRecord(String topic, String key, K k, V v, MessageId id, long publishTime, Map<String,String> props) {
        this.topic = topic; this.key = key; this.k = k; this.v = v; this.messageId = id; this.publishTime = publishTime; this.properties = props;
    }
}