package com.evolve.integration.kafka.kop;

import org.apache.pulsar.client.api.MessageId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demo: assign monotonic offsets per topic as messages come in and store the mapping.
 * Real KoP persists mapping and supports compaction, recovery, multi-partition topics.
 */
public class OffsetManager {
    private final Map<String, AtomicLong> topicOffsets = new ConcurrentHashMap<>();
    private final Map<String, Map<Long, MessageId>> mapping = new ConcurrentHashMap<>();

    public long record(String topic, MessageId msgId) {
        AtomicLong off = topicOffsets.computeIfAbsent(topic, t -> new AtomicLong(0));
        long offset = off.getAndIncrement();
        mapping.computeIfAbsent(topic, t -> new ConcurrentHashMap<>()).put(offset, msgId);
        return offset;
    }

    public MessageId lookup(String topic, long offset) {
        Map<Long, MessageId> m = mapping.get(topic);
        if (m == null) return null;
        return m.get(offset);
    }
}