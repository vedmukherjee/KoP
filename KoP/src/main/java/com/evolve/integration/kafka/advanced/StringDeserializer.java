package com.evolve.integration.kafka.advanced;

import java.nio.charset.StandardCharsets;

public class StringDeserializer implements Deserializer<String> { public String deserialize(byte[] b){ return b==null?null:new String(b, StandardCharsets.UTF_8);} }