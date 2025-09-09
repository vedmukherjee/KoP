package com.evolve.integration.kafka.extended.consumer;

import org.apache.pulsar.client.api.*;

import java.util.Properties;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class KafkaConsumer implements AutoCloseable {

    private final PulsarClient client;
    private final Consumer<byte[]> consumer;

    public KafkaConsumer(Properties props) throws PulsarClientException {
        String serviceUrl = props.getProperty("bootstrap.servers", "pulsar://localhost:6650");
        String topic = props.getProperty("topic");
        String groupId = props.getProperty("group.id", "default-group");
        String reset = props.getProperty("auto.offset.reset", "earliest");

        SubscriptionInitialPosition pos = "latest".equalsIgnoreCase(reset)
                ? SubscriptionInitialPosition.Latest
                : SubscriptionInitialPosition.Earliest;

        this.client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        this.consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(groupId)
                .subscriptionInitialPosition(pos)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
    }

    /**
     * Poll with blocking receive (default).
     */
    public Message<byte[]> poll(long timeoutMillis) throws PulsarClientException {
        try {
            return consumer.receive((int) timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (PulsarClientException e) {
            throw e;
        }
    }

    /**
     * Convenience: poll with default 5s timeout, returning Optional.
     */
    public Optional<Message<byte[]>> pollOnce() throws PulsarClientException {
        Message<byte[]> msg = poll(5000);
        return Optional.ofNullable(msg);
    }

    /**
     * Acknowledge (manual commit) using Message object.
     */
    public void commit(Message<byte[]> message) throws PulsarClientException {
        consumer.acknowledge(message);
    }

    /**
     * Negative acknowledge (so Pulsar will redeliver).
     */
    public void negativeAcknowledge(Message<byte[]> message) {
        consumer.negativeAcknowledge(message);
    }

    /**
     * Close consumer & client.
     */
    @Override
    public void close() throws PulsarClientException {
        if (consumer != null) consumer.close();
        if (client != null) client.close();
    }
}
