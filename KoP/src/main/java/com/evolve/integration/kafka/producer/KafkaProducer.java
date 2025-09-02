package com.evolve.integration.kafka.producer;

import org.apache.pulsar.client.api.*;

public class KafkaProducer implements AutoCloseable {
    private final PulsarClient client;
    private final Producer<byte[]> producer;

    public KafkaProducer(String serviceUrl, String topic) throws PulsarClientException {
        this.client = PulsarClient.builder()
                .serviceUrl(serviceUrl) // e.g. pulsar://localhost:6650
                .build();

        this.producer = client.newProducer()
                .topic(topic) // e.g. persistent://public/default/my-topic
                .create();
    }

    public void send(String key, String value) throws PulsarClientException {
        producer.newMessage()
                .key(key)
                .value(value.getBytes())
                .send();
    }

    @Override
    public void close() throws PulsarClientException {
        producer.close();
        client.close();
    }
}

