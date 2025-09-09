package com.evolve.integration.KafkaApp;

import org.junit.jupiter.api.*;

import com.evolve.integration.kafka.basic.consumer.KafkaConsumer;
import com.evolve.integration.kafka.basic.producer.KafkaProducer;

import java.util.Properties;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KoPIntermediateTest {

    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/my-topic";

    private Properties baseProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", SERVICE_URL);
        props.put("topic", TOPIC);
        return props;
    }

    private Properties baseConsumerProps(String groupId, String reset) {
        Properties props = new Properties();
        props.put("bootstrap.servers", SERVICE_URL);
        props.put("topic", TOPIC);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", reset);
        return props;
    }

    // 🔹 1. Simple Produce & Consume
    @Test
    @Order(1)
    public void testProduceAndConsume() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(baseProducerProps());
             KafkaConsumer consumer = new KafkaConsumer(baseConsumerProps("group1", "earliest"))) {

            producer.send("k1", "hello");
            consumer.poll();
        }
    }

    // 🔹 2. Async Send
    /*@Test
    @Order(2)
    public void testAsyncSend() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(baseProducerProps())) {
            producer.sendAsync("k2", "async-message");
            Thread.sleep(1000); // wait for callback
        }
    }

    // 🔹 3. Key-based Partitioning
    @Test
    @Order(3)
    public void testKeyPartitioning() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(baseProducerProps())) {
            producer.send("user-1", "event-A");
            producer.send("user-1", "event-B");
        }
        // Partition check can be done with PulsarAdmin (not shown here)
    }

    // 🔹 4. Batching & Linger
    @Test
    @Order(4)
    public void testBatching() throws Exception {
        Properties props = baseProducerProps();
        props.put("batch.size", "10");
        props.put("linger.ms", "50");

        try (KafkaProducer producer = new KafkaProducer(props)) {
            for (int i = 0; i < 20; i++) {
                producer.sendAsync("batch-key", "msg-" + i);
            }
            Thread.sleep(2000); // let batching happen
        }
    }

    // 🔹 5. Compression
    @Test
    @Order(5)
    public void testCompression() throws Exception {
        Properties props = baseProducerProps();
        // Pulsar supports: LZ4, ZLIB, ZSTD, SNAPPY
        props.put("compression.type", "LZ4");

        try (KafkaProducer producer = new KafkaProducer(props)) {
            producer.send("c-key", "compressed-message");
        }
    }

    // 🔹 6. Earliest Offset
    @Test
    @Order(6)
    public void testEarliestOffsetConsumer() throws Exception {
        try (KafkaConsumer consumer = new KafkaConsumer(baseConsumerProps("group2", "earliest"))) {
            consumer.poll(); // should see old messages
        }
    }

    // 🔹 7. Latest Offset
    @Test
    @Order(7)
    public void testLatestOffsetConsumer() throws Exception {
        try (KafkaConsumer consumer = new KafkaConsumer(baseConsumerProps("group3", "latest"))) {
            // produce after subscription
            try (KafkaProducer producer = new KafkaProducer(baseProducerProps())) {
                producer.send("latest-key", "new-message");
            }
            consumer.poll(); // should only see new message
        }
    }

    // 🔹 8. Manual Acknowledge (commit)
    @Test
    @Order(8)
    public void testManualAcknowledge() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(baseProducerProps());
             KafkaConsumer consumer = new KafkaConsumer(baseConsumerProps("group4", "earliest"))) {

            producer.send("ack-key", "ack-message");

            // poll will auto-ack inside wrapper; you can extend wrapper for manual ack tests
            consumer.poll();
        }
    }

    // 🔹 9. Multiple Consumers (Group)
    @Test
    @Order(9)
    public void testConsumerGroupDistribution() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(baseProducerProps());
             KafkaConsumer consumer1 = new KafkaConsumer(baseConsumerProps("group5", "earliest"));
             KafkaConsumer consumer2 = new KafkaConsumer(baseConsumerProps("group5", "earliest"))) {

            producer.send("cg-key", "group-message-1");
            producer.send("cg-key", "group-message-2");

            consumer1.poll();
            consumer2.poll();
        }
    }

    // 🔹 10. Transactions (simulate Kafka exactly-once)
    @Test
    @Order(10)
    public void testTransactionalProducerConsumer() throws Exception {
        // Pulsar supports transactions via Transaction API
        // This would require extending wrapper to add transaction begin/commit/abort
        // For now, simulate by producing then consuming
        try (KafkaProducer producer = new KafkaProducer(baseProducerProps());
             KafkaConsumer consumer = new KafkaConsumer(baseConsumerProps("group6", "earliest"))) {

            producer.send("txn-key", "txn-message");
            consumer.poll();
        }
    }

    // 🔹 11. Idempotence-like Behavior
    @Test
    @Order(11)
    public void testIdempotenceSimulation() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(baseProducerProps());
             KafkaConsumer consumer = new KafkaConsumer(baseConsumerProps("group7", "earliest"))) {

            producer.send("idem-key", "msg-1");
            producer.send("idem-key", "msg-1"); // duplicate

            consumer.poll(); // should receive both, dedup logic must be app side
        }
    }*/
}

