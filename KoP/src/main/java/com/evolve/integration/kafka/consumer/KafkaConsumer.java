package com.evolve.integration.kafka.consumer;

import org.apache.pulsar.client.api.*;

public class KafkaConsumer implements AutoCloseable {
    private final PulsarClient client;
    private final Consumer<byte[]> consumer;

    public KafkaConsumer(String serviceUrl, String topic, String groupId) throws PulsarClientException {
        this.client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        this.consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(groupId) // groupId ~ subscriptionName
                .subscriptionType(SubscriptionType.Shared) // like Kafka consumer group
                .subscribe();
    }

    public void poll() throws PulsarClientException {
        while (true) {
            Message<byte[]> msg = consumer.receive();
            try {
                System.out.printf("Consumed key=%s, value=%s, messageId=%s%n",
                        msg.getKey(),
                        new String(msg.getValue()),
                        msg.getMessageId());
                consumer.acknowledge(msg);
            } catch (Exception e) {
                consumer.negativeAcknowledge(msg);
            }
        }
    }

    @Override
    public void close() throws PulsarClientException {
        consumer.close();
        client.close();
    }
}

