package com.evolve.integration.kafka.basic.consumer;

import org.apache.pulsar.client.api.*;

import java.util.Properties;

public class KafkaConsumer implements AutoCloseable {
    private final PulsarClient client;
    private final Consumer<byte[]> consumer;

    public KafkaConsumer(Properties props) throws PulsarClientException {
        String serviceUrl = props.getProperty("bootstrap.servers", "pulsar://localhost:6650");
        String topic = props.getProperty("topic");
        String groupId = props.getProperty("group.id", "default-group");
        String offsetReset = props.getProperty("auto.offset.reset", "earliest");

        this.client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        ConsumerBuilder<byte[]> builder = client.newConsumer()
                .topic(topic)
                .subscriptionName(groupId)
                .subscriptionType(SubscriptionType.Shared); // like Kafka group

        if (offsetReset.equals("earliest")) {
            builder.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        } else {
            builder.subscriptionInitialPosition(SubscriptionInitialPosition.Latest);
        }

        this.consumer = builder.subscribe();
    }

    public void poll() throws PulsarClientException {
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

    @Override
    public void close() throws PulsarClientException {
        consumer.close();
        client.close();
    }
}

