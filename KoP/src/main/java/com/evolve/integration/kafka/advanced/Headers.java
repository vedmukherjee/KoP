package com.evolve.integration.kafka.advanced;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Headers implements Iterable<Header> {
    private final List<Header> list = new ArrayList<>();
    public Headers add(String key, byte[] value) { list.add(new Header(key, value)); return this; }
    public Iterator<Header> iterator() { return list.iterator(); }
}
