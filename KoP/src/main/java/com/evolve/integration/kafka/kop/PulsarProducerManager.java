package com.evolve.integration.kafka.kop;


import org.apache.pulsar.client.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class PulsarProducerManager {
    private final PulsarClient client;
    private final Map<String, Producer<byte[]>> producers = new ConcurrentHashMap<>();

    public PulsarProducerManager(String serviceUrl) throws PulsarClientException {
        client = PulsarClient.builder().serviceUrl(serviceUrl).build();
    }

    public CompletableFuture<MessageId> sendAsync(String topic, byte[] payload) throws PulsarClientException {
        Producer<byte[]> p = producers.computeIfAbsent(topic, t -> {
            try {
                return client.newProducer()
                        .topic(t)
                        .create();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
        return p.sendAsync(payload).thenApply(mid -> (MessageId) mid);
    }

    public void send(String topic, String message) throws PulsarClientException {
        Producer<byte[]> producer = producers.computeIfAbsent(topic, t -> {
            try {
                return client.newProducer()
                        .topic(t)
                        .create();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
        producer.send(message.getBytes(StandardCharsets.UTF_8));
    }
    
    public void close() throws PulsarClientException {
        producers.values().forEach(p -> { try { p.close(); } catch (Exception ignore) {} });
        client.close();
    }
}
