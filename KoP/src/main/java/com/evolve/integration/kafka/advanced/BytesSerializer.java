package com.evolve.integration.kafka.advanced;

public class BytesSerializer implements Serializer<byte[]> { public byte[] serialize(byte[] b){ return b; } }

