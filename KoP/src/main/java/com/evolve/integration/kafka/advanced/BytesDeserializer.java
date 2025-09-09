package com.evolve.integration.kafka.advanced;

public class BytesDeserializer implements Deserializer<byte[]> { public byte[] deserialize(byte[] b){ return b; } }