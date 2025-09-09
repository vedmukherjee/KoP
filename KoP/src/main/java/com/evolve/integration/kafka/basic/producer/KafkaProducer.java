package com.evolve.integration.kafka.basic.producer;

import org.apache.pulsar.client.api.*;

import java.util.Properties;

public class KafkaProducer implements AutoCloseable {
    private final PulsarClient client;
    private final Producer<byte[]> producer;

    public KafkaProducer(Properties props) throws PulsarClientException {
        String serviceUrl = props.getProperty("bootstrap.servers", "pulsar://localhost:6650");
        String topic = props.getProperty("topic");

        this.client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        ProducerBuilder<byte[]> builder = client.newProducer()
                .topic(topic)
                .enableBatching(Boolean.parseBoolean(props.getProperty("enable.batching", "true")))
                .batchingMaxMessages(Integer.parseInt(props.getProperty("batch.size", "1000")))
                .batchingMaxPublishDelay(Long.parseLong(props.getProperty("linger.ms", "1")), java.util.concurrent.TimeUnit.MILLISECONDS);

        // Pulsar always persists synchronously, no exact Kafka acks, but similar semantics.
        if (Boolean.parseBoolean(props.getProperty("enable.blocking", "false"))) {
            builder.blockIfQueueFull(true);
        }

        this.producer = builder.create();
    }

    public void send(String key, String value) throws PulsarClientException {
        producer.newMessage()
                .key(key)
                .value(value.getBytes())
                .send(); // blocking send for ack=all-like semantics
    }

    public void sendAsync(String key, String value) {
        producer.newMessage()
                .key(key)
                .value(value.getBytes())
                .sendAsync()
                .thenAccept(msgId -> System.out.println("Sent message ID: " + msgId));
    }

    @Override
    public void close() throws PulsarClientException {
        producer.close();
        client.close();
    }
}
