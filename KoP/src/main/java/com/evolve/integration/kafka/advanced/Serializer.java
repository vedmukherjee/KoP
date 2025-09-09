package com.evolve.integration.kafka.advanced;

interface Serializer<T> { byte[] serialize(T t); }
