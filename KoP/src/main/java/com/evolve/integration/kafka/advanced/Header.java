package com.evolve.integration.kafka.advanced;

public class Header {
    public final String key; public final byte[] value;
    public Header(String key, byte[] value) { this.key = key; this.value = value; }
}
