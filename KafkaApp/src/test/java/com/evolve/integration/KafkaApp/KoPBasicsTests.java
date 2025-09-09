package com.evolve.integration.KafkaApp;

import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.evolve.integration.kafka.basic.consumer.KafkaConsumer;
import com.evolve.integration.kafka.basic.producer.KafkaProducer;

@SpringBootTest
class KoPBasicsTests {

    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/my-topic";

    @Test
    public void testProduceAndConsumeMessage() throws Exception {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", SERVICE_URL);
        producerProps.put("topic", TOPIC);

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", SERVICE_URL);
        consumerProps.put("topic", TOPIC);
        consumerProps.put("group.id", "test-group");
        consumerProps.put("auto.offset.reset", "earliest");

        try (KafkaProducer producer = new KafkaProducer(producerProps);
             KafkaConsumer consumer = new KafkaConsumer(consumerProps)) {

            producer.send("key1", "hello-world");

            // poll once
            consumer.poll();
        }
    }
	@Test
    public void testProduceWithKeyPartitioning() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", SERVICE_URL);
        props.put("topic", TOPIC);

        try (KafkaProducer producer = new KafkaProducer(props)) {
            producer.send("user-1", "event-1");
            producer.send("user-1", "event-2");
        }
        // You can add partition assertion using Pulsar admin client
    }

}
