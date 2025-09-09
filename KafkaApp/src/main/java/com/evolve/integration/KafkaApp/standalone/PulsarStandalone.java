package com.evolve.integration.KafkaApp.standalone;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarStandalone {

    private static final String SERVICE_URL = "pulsar://localhost:6650"; // Adjust if your standalone instance runs on a different port/host
    private static final String TOPIC_NAME = "my-topic-1";

    public static void main(String[] args) {
        try {
            // Create a Pulsar client
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .build();

            // Create a Producer
            Producer<byte[]> producer = client.newProducer()
                    .topic(TOPIC_NAME)
                    .create();

            // Send a message
            String messageContent = "Hello from standalone Pulsar!";
            producer.send(messageContent.getBytes());
            System.out.println("Message sent: " + messageContent);

            // Create a Consumer
            Consumer<byte[]> consumer = client.newConsumer()
                    .topic(TOPIC_NAME)
                    .subscriptionName("my-subscription")
                    .subscribe();

            // Receive a message
            Message<byte[]> msg = consumer.receive();
            try {
                System.out.println("Message received: " + new String(msg.getData()));
                consumer.acknowledge(msg); // Acknowledge the message
            } finally {
                msg.release(); // Release the message
            }

            // Close resources
            consumer.close();
            producer.close();
            client.close();

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }
}