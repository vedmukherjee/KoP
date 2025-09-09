package com.evolve.integration.kafka.advanced;

public class ProducerRecord<K,V> {
    public final String topic; public final K key; public final V value; public final Headers headers;
    public ProducerRecord(String topic, K key, V value) { this(topic, key, value, new Headers()); }
    public ProducerRecord(String topic, K key, V value, Headers headers) {
        this.topic = topic; this.key = key; this.value = value; this.headers = headers;
    }
}