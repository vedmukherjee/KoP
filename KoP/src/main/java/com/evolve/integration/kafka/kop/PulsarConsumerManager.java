package com.evolve.integration.kafka.kop;

import org.apache.pulsar.client.api.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PulsarConsumerManager {
    private final PulsarClient client;
    private final Map<String, Consumer<byte[]>> consumers = new HashMap<>();

    public PulsarConsumerManager(String serviceUrl) throws PulsarClientException {
        client = PulsarClient.builder().serviceUrl(serviceUrl).build();
    }

    public List<byte[]> receiveBatch(String topic, int maxMessages, long timeoutMs) throws PulsarClientException {
        Consumer<byte[]> c = consumers.computeIfAbsent(topic, t -> {
            try {
                return client.newConsumer()
                        .topic(t)
                        .subscriptionName("kop-demo-sub")
                        .subscriptionType(SubscriptionType.Shared)
                        .subscribe();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });

        List<byte[]> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && out.size() < maxMessages) {
            try {
                Message<byte[]> m = c.receive(100, TimeUnit.MILLISECONDS);
                if (m != null) {
                    out.add(m.getValue());
                    c.acknowledge(m);
                }
            } catch (Exception e) {
                break;
            }
        }
        return out;
    }

    public void close() throws PulsarClientException {
        consumers.values().forEach(c -> { try { c.close(); } catch (Exception ignore) {} });
        client.close();
    }
}
