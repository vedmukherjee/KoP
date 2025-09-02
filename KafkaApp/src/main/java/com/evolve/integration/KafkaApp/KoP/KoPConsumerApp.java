package com.evolve.integration.KafkaApp;

import com.evolve.integration.kafka.consumer.KafkaConsumer;

public class ConsumerApp {
    public static void main(String[] args) throws Exception {
        try (KafkaConsumer consumer = new KafkaConsumer("pulsar://localhost:6650", "my-topic", "my-group")) {
            consumer.poll();
        }
    }
}

