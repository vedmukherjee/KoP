package com.evolve.integration.KafkaApp;

// SPDX-License-Identifier: MIT
// File: src/test/java/io/example/kafka/KafkaIntegrationTest.java

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.*;

/*import com.evolve.integration.kafka.advanced.ConsumerRecord;
import com.evolve.integration.kafka.advanced.ConsumerRecords;
import com.evolve.integration.kafka.advanced.KafkaConsumer;
import com.evolve.integration.kafka.advanced.KafkaProducer;
import com.evolve.integration.kafka.advanced.ProducerRecord;
import com.evolve.integration.kafka.advanced.RecordMetadata;*/


import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaUnitTest {

    private static final String BOOTSTRAP = "localhost:9092"; // standalone Kafka
    private static final String TOPIC = "test-topic-enterprise";
    private static final String GROUP = "test-group-enterprise";

    //private static final String BOOTSTRAP = "pulsar://localhost:6650";
    //private static final String TOPIC = "persistent://public/default/my-topic";

    private static AdminClient admin;

    @BeforeAll
    static void setup() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        admin = AdminClient.create(adminProps);

        try {
            NewTopic newTopic = new NewTopic(TOPIC, 3, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (Exception e) {
            // Ignore if already exists
        }
    }

    @AfterAll
    static void teardown() {
        try {
            admin.deleteTopics(Collections.singleton(TOPIC)).all().get();
        } catch (Exception ignored) {
        }
        admin.close();
    }

    @Test
    @Order(1)
    void testProduceMessages() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        props.put("acks", "all");
        props.put("topic", TOPIC);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, "key-" + i, "value-" + i);
                RecordMetadata metadata = producer.send(record).get(5, TimeUnit.SECONDS);
                assertNotNull(metadata);
            }
        }
    }

    @Test
    @Order(2)
    void testConsumeMessages() throws PulsarClientException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "GROUP1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("topic", TOPIC);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            assertFalse(records.count() == 0, "Should consume at least one record");
            for (ConsumerRecord<String, String> record : records) {
                //System.out.println(record.k);
                //System.out.println(record.v);
                assertTrue(record.key()!=null);
                assertTrue(record.value()!=null);
            }
        }
    }

    @Test
    @Order(3)
    void testListTopicsAndOffsets() throws Exception {
        // List topics
        Set<String> topics = admin.listTopics().names().get();
        assertTrue(topics.contains(TOPIC));

        // Get offsets
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            request.put(new TopicPartition(TOPIC, i), OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                admin.listOffsets(request).all().get();

        assertEquals(3, offsets.size());
        offsets.forEach((tp, info) -> assertTrue(info.offset() >= 0));
    }
    @Test
    @Order(4)
    void testProduceToInvalidBroker() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999"); // invalid broker
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        assertThrows(ExecutionException.class, () -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                producer.send(new ProducerRecord<>(TOPIC, "keyX", "valueX")).get(3, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    @Order(5)
    void testConsumeFromNonExistentTopic() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "invalid-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singleton("non-existent-topic"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            assertTrue(records.isEmpty(), "Should not receive any records from non-existent topic");
        }
    }

    @Test
    @Order(6)
    void testProduceWithInvalidSerializer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.NonExistentSerializer");

        assertThrows(ClassNotFoundException.class, () -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                producer.send(new ProducerRecord<>(TOPIC, "badKey", "badValue"));
            }
        });
    }

    @Test
    @Order(7)
    void testAdminDescribeInvalidTopic() {
        assertThrows(ExecutionException.class, () -> {
            admin.describeTopics(Collections.singleton("does-not-exist"))
                 .all()
                 .get(3, TimeUnit.SECONDS);
        });
    }

    @Test
    @Order(8)
    void testCommitOffsetOnClosedConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP + "-offset-test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.close();

        assertThrows(IllegalStateException.class, () -> {
            consumer.commitSync();
        });
    }
}
