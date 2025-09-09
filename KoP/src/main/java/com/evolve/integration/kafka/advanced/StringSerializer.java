package com.evolve.integration.kafka.advanced;

import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String> { public byte[] serialize(String s){ return s==null?null:s.getBytes(StandardCharsets.UTF_8);} }

