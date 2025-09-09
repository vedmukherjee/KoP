package com.evolve.integration.KafkaApp.KoP;

import java.util.Properties;

import com.evolve.integration.kafka.basic.consumer.KafkaConsumer;


public class KoPConsumerApp {
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/my-topic";
    public static void main(String[] args) throws Exception {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", SERVICE_URL);
        consumerProps.put("topic", TOPIC);
        consumerProps.put("group.id", "test-group");
        consumerProps.put("auto.offset.reset", "earliest");
        try (KafkaConsumer consumer = new KafkaConsumer(consumerProps)) {
            consumer.poll();
        }
    }
}

