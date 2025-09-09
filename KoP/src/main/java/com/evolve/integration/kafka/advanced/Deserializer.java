package com.evolve.integration.kafka.advanced;

interface Deserializer<T> { T deserialize(byte[] bytes); }