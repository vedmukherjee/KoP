package com.evolve.integration.kafka.advanced;

import java.util.Iterator;
import java.util.List;

public class ConsumerRecords<K,V> implements Iterable<ConsumerRecord<K,V>> {
    private final List<ConsumerRecord<K,V>> inner;
    ConsumerRecords(List<ConsumerRecord<K,V>> inner) { this.inner = inner; }
    public int count() { return inner.size(); }
    public Iterator<ConsumerRecord<K,V>> iterator() { return inner.iterator(); }
}