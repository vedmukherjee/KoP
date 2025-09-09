package com.evolve.integration.KafkaApp;

import com.evolve.integration.kafka.advanced.ProducerRecord;
import com.evolve.integration.kafka.advanced.ConsumerRecord;
import com.evolve.integration.kafka.advanced.ConsumerRecords;
import com.evolve.integration.kafka.advanced.StringSerializer;
import com.evolve.integration.kafka.advanced.StringDeserializer;
import com.evolve.integration.kafka.advanced.Headers;

import org.junit.jupiter.api.*;

import com.evolve.integration.kafka.advanced.KafkaLikeWrappers;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for KafkaLikeWrappers using Testcontainers + Pulsar.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KoPProductionGradeTest {

    private String serviceUrl="pulsar://localhost:6650";

    private Properties baseProducerProps(String topic) {
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", serviceUrl);
        p.setProperty("topic", topic);
        p.setProperty("enable.batching", "true");
        p.setProperty("batch.size", "100");
        p.setProperty("linger.ms", "5");
        p.setProperty("compression.type", "LZ4");
        p.setProperty("enable.idempotence", "true");
        p.setProperty("retries", "3");
        p.setProperty("retry.backoff.ms", "50");
        return p;
    }

    private Properties baseConsumerProps(String topic, String groupId) {
        Properties c = new Properties();
        c.setProperty("bootstrap.servers", serviceUrl);
        c.setProperty("topic", topic);
        c.setProperty("group.id", groupId);
        c.setProperty("auto.offset.reset", "earliest");
        c.setProperty("isolation.level", "read_committed");
        return c;
    }

    @Test
    void testSimpleProduceConsume() throws Exception {
        String topic = "persistent://public/default/simple";

        try (KafkaLikeWrappers.KafkaProducer<String, String> prod =
                     new KafkaLikeWrappers.KafkaProducer<>(baseProducerProps(topic),
                             new StringSerializer(), new StringSerializer());
             KafkaLikeWrappers.KafkaConsumer<String, String> cons =
                     new KafkaLikeWrappers.KafkaConsumer<>(baseConsumerProps(topic, "g1"),
                             new StringDeserializer(), new StringDeserializer())) {

            prod.sendSync(new ProducerRecord<>(topic, "k1", "v1"));

            ConsumerRecords<String, String> recs = cons.poll(Duration.ofSeconds(2));
            assertEquals(1, recs.count());
            ConsumerRecord<String, String> rec = recs.iterator().next();
            assertEquals("k1", rec.key);
            assertEquals("v1", rec.v);
        }
    }

    @Test
    void testRetryAndIdempotence() throws Exception {
        String topic = "persistent://public/default/retry";

        try (KafkaLikeWrappers.KafkaProducer<String, String> prod =
                     new KafkaLikeWrappers.KafkaProducer<>(baseProducerProps(topic),
                             new StringSerializer(), new StringSerializer());
             KafkaLikeWrappers.KafkaConsumer<String, String> cons =
                     new KafkaLikeWrappers.KafkaConsumer<>(baseConsumerProps(topic, "g2"),
                             new StringDeserializer(), new StringDeserializer())) {

            for (int i = 0; i < 5; i++) {
                prod.sendSync(new ProducerRecord<>(topic, "k" + i, "v" + i));
            }

            ConsumerRecords<String, String> recs = cons.poll(Duration.ofSeconds(2));
            assertTrue(recs.count() >= 5);
        }
    }

    @Test
    void testHeadersRoundTrip() throws Exception {
        String topic = "persistent://public/default/headers";

        try (KafkaLikeWrappers.KafkaProducer<String, String> prod =
                     new KafkaLikeWrappers.KafkaProducer<>(baseProducerProps(topic),
                             new StringSerializer(), new StringSerializer());
             KafkaLikeWrappers.KafkaConsumer<String, String> cons =
                     new KafkaLikeWrappers.KafkaConsumer<>(baseConsumerProps(topic, "g3"),
                             new StringDeserializer(), new StringDeserializer())) {

            Headers h = new Headers().add("traceId", "abc123".getBytes());
            prod.sendSync(new ProducerRecord<>(topic, "k1", "v1", h));

            ConsumerRecords<String, String> recs = cons.poll(Duration.ofSeconds(2));
            ConsumerRecord<String, String> rec = recs.iterator().next();
            assertEquals("v1", rec.v);
            assertTrue(rec.properties.containsKey("hdr_traceId"));
        }
    }

    @Test
    void testTransactions() throws Exception {
        String topic = "persistent://public/default/txn";

        Properties props = baseProducerProps(topic);
        props.setProperty("enable.transactions", "true");

        try (KafkaLikeWrappers.KafkaProducer<String, String> prod =
                     new KafkaLikeWrappers.KafkaProducer<>(props,
                             new StringSerializer(), new StringSerializer());
             KafkaLikeWrappers.KafkaConsumer<String, String> cons =
                     new KafkaLikeWrappers.KafkaConsumer<>(baseConsumerProps(topic, "g4"),
                             new StringDeserializer(), new StringDeserializer())) {

            prod.initTransactions();
            prod.beginTransaction();
            prod.sendSync(new ProducerRecord<>(topic, "k1", "v1"));
            prod.commitTransaction();

            ConsumerRecords<String, String> recs = cons.poll(Duration.ofSeconds(2));
            assertEquals(1, recs.count());
        }
    }
}

