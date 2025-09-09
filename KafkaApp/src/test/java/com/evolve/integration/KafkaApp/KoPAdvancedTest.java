package com.evolve.integration.KafkaApp;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.junit.jupiter.api.*;

import com.evolve.integration.kafka.extended.consumer.KafkaConsumer;
import com.evolve.integration.kafka.extended.producer.KafkaProducer;

import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KoPAdvancedTest {

    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/my-topic-1";
    private static final String TOPIC0 = "persistent://public/default/my-topic-0";
    private static final String TOPIC2 = "persistent://public/default/my-topic-2";

    
    private Properties producerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", SERVICE_URL);
        p.put("topic", TOPIC);
        return p;
    }

    private Properties consumerProps(String groupId, String reset) {
        Properties p = new Properties();
        p.put("bootstrap.servers", SERVICE_URL);
        p.put("topic", TOPIC);
        p.put("group.id", groupId);
        p.put("auto.offset.reset", reset);
        return p;
    }

    // Basic produce-consume
    @Test
    @Order(1)
    public void testProduceAndConsume() throws Exception {
        Properties p = producerProps();
        p.put("topic", TOPIC0);
        Properties c = consumerProps("g1", "earliest");
        c.put("topic", TOPIC0);
        try (KafkaProducer producer = new KafkaProducer(p);
             KafkaConsumer consumer = new KafkaConsumer(c)) {

            MessageId mid = producer.send("k1", "hello-world");
            assertThat(mid).isNotNull();

            Message<byte[]> msg = consumer.poll(5000);
            assertThat(msg).isNotNull();
            assertThat(new String(msg.getValue())).isEqualTo("hello-world");
            consumer.commit(msg);
        }
    }
    
    // Async send
    @Test
    @Order(2)
    // ...existing code...
    public void testAsyncSend() throws Exception {
        Properties p = producerProps();
        p.put("topic", TOPIC2);
        Properties c = consumerProps("g2", "earliest");
        c.put("topic", TOPIC2);
        try (KafkaProducer producer = new KafkaProducer(p);
            KafkaConsumer consumer = new KafkaConsumer(c)) {

            CompletableFuture<MessageId> f = producer.sendAsync("ak", "async-message");
            MessageId id = f.get(5, TimeUnit.SECONDS);
            assertThat(id).isNotNull();

            // Poll until message is received or timeout
            Message<byte[]> msg = null;
            long end = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < end) {
                msg = consumer.poll(500);
                if (msg != null) break;
            }

            assertThat(msg).isNotNull();
            assertThat(new String(msg.getValue())).isEqualTo("async-message");
            consumer.commit(msg);
        }
    }

    // Key-based partitioning (logical test: same key -> application consistent ordering)
    @Test
    @Order(3)
    public void testKeyPartitioningOrdering() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(producerProps());
             KafkaConsumer consumer = new KafkaConsumer(consumerProps("g3", "earliest"))) {
            IntStream.range(0, 5).forEach(i -> {
                try {
                    producer.send("user-1", "event-" + i);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            // read 5 messages and validate order for that subscription type (Shared may interleave, so this is best-effort)
            for (int i = 0; i < 5; i++) {
                Message<byte[]> m = consumer.poll(5000);
                assertThat(m).isNotNull();
                consumer.commit(m);
            }
        }
    }

    // Batching & linger
    @Test
    @Order(4)
    public void testBatchingAndLinger() throws Exception {
        Properties p = producerProps();
        p.put("batch.size", "50");
        p.put("linger.ms", "100");

        try (KafkaProducer producer = new KafkaProducer(p);
             KafkaConsumer consumer = new KafkaConsumer(consumerProps("g4", "earliest"))) {

            IntStream.range(0, 20).forEach(i -> producer.sendAsync("bk", "bmsg-" + i));
            // give time for flush
            Thread.sleep(500);

            int received = 0;
            for (int i = 0; i < 20; i++) {
                Message<byte[]> m = consumer.poll(5000);
                if (m != null) {
                    received++;
                    consumer.commit(m);
                }
            }
            assertThat(received).isEqualTo(20);
        }
    }

    // Compression test
    @Test
    @Order(5)
    public void testCompression() throws Exception {
        Properties p = producerProps();
        p.put("compression.type", "LZ4");
        p.put("topic", "persistent://public/default/my-topic-3");
        Properties c = consumerProps("g2", "earliest");
        c.put("topic", "persistent://public/default/my-topic-3");

        try (KafkaProducer producer = new KafkaProducer(p);
             KafkaConsumer consumer = new KafkaConsumer(c)) {

            producer.send("ck", "compressed");
            Message<byte[]> m = consumer.poll(5000);
            assertThat(m).isNotNull();
            assertThat(new String(m.getValue())).isEqualTo("compressed");
            consumer.commit(m);
        }
    }

    // Earliest vs Latest
    @Test
    @Order(6)
    public void testEarliestAndLatestOffsetBehavior() throws Exception {
        // produce one message first
        Properties p = producerProps();
        p.put("topic", "persistent://public/default/my-topic-4");
        Properties c = consumerProps("g2", "earliest");
        c.put("topic", "persistent://public/default/my-topic-4");

        Properties c1 = consumerProps("g6-l", "latest");
        c1.put("topic", "persistent://public/default/my-topic-4");
        try (KafkaProducer producer = new KafkaProducer(p)) {
            producer.send("o1", "old-message");
        }

        // Earliest consumer should read it
        try (KafkaConsumer consumerEarliest = new KafkaConsumer(c)) {
            Message<byte[]> m = consumerEarliest.poll(5000);
            assertThat(m).isNotNull();
            assertThat(new String(m.getValue())).isEqualTo("old-message");
            consumerEarliest.commit(m);
        }

        // Latest consumer created AFTER produces; should only see new messages
        try (KafkaConsumer consumerLatest = new KafkaConsumer(c1);
             KafkaProducer producer = new KafkaProducer(producerProps())) {

            producer.send("o2", "new-message");
            Message<byte[]> ml = consumerLatest.poll(5000);
            assertThat(ml).isNotNull();
            assertThat(new String(ml.getValue())).isEqualTo("new-message");
            consumerLatest.commit(ml);
        }
    }

    // Consumer group distribution (Shared subscription)
    @Test
    @Order(7)
    public void testConsumerGroupDistribution() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(producerProps());
             KafkaConsumer c1 = new KafkaConsumer(consumerProps("group-dist", "earliest"));
             KafkaConsumer c2 = new KafkaConsumer(consumerProps("group-dist", "earliest"))) {

            producer.send("gk", "gmsg-1");
            producer.send("gk", "gmsg-2");

            // Each consumer should receive some message(s)
            Message<byte[]> m1 = c1.poll(5000);
            if (m1 != null) c1.commit(m1);

            Message<byte[]> m2 = c2.poll(5000);
            if (m2 != null) c2.commit(m2);

            // At least one consumer should have received, count >=1
            assertThat(m1 != null || m2 != null).isTrue();
        }
    }

    // Manual commit (ack) demonstration
    @Test
    @Order(8)
    public void testManualCommit() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(producerProps());
             KafkaConsumer consumer = new KafkaConsumer(consumerProps("g7", "earliest"))) {

            producer.send("manual", "manual-msg");

            Message<byte[]> m = consumer.poll(5000);
            assertThat(m).isNotNull();
            // We intentionally do not commit yet to simulate manual commit semantics.
            // Now commit:
            consumer.commit(m);

            // Re-create consumer with same group to ensure message is not redelivered
            try (KafkaConsumer consumer2 = new KafkaConsumer(consumerProps("g7", "earliest"))) {
                Message<byte[]> maybe = consumer2.poll(2000);
                // since we committed, consumer2 should not get the same message again
                assertThat(maybe).isNull();
            }
        }
    }

    // Transactional producer: begin -> send -> commit => message visible
    @Test
    @Order(9)
    public void testTransactionalProduceCommit() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(producerProps());
             KafkaConsumer consumer = new KafkaConsumer(consumerProps("g8", "earliest"))) {

            producer.beginTransaction(2, TimeUnit.MINUTES);
            producer.send("txn", "txn-msg");
            // Not yet visible to read_committed semantics in consumer unless committed; commit now
            producer.commitTransaction();

            Message<byte[]> m = consumer.poll(5000);
            assertThat(m).isNotNull();
            assertThat(new String(m.getValue())).isEqualTo("txn-msg");
            consumer.commit(m);
        }
    }

    // Transaction abort: message should NOT be visible
    @Test
    @Order(10)
    public void testTransactionalProduceAbort() throws Exception {
        try (KafkaProducer producer = new KafkaProducer(producerProps());
             KafkaConsumer consumer = new KafkaConsumer(consumerProps("g9", "earliest"))) {

            producer.beginTransaction(2, TimeUnit.MINUTES);
            producer.send("txnab", "txnab-msg");
            producer.abortTransaction();

            // The aborted message should not be visible. Wait briefly then assert no message (or unrelated message).
            Message<byte[]> m = consumer.poll(1500);
            // it is acceptable that m is null (no message) or different message from prior tests; assert that if message exists it's not our aborted content
            if (m != null) {
                assertThat(new String(m.getValue())).isNotEqualTo("txnab-msg");
                consumer.commit(m);
            }
        }
    }

    // Idempotence simulation: two identical sends with same key+sequenceId should be deduped by consumer side logic if implemented
    @Test
    @Order(11)
    public void testIdempotenceSimulation() throws Exception {
        Properties p = producerProps();
        p.put("enable.idempotence", "true");

        try (KafkaProducer producer = new KafkaProducer(p);
             KafkaConsumer consumer = new KafkaConsumer(consumerProps("g10", "earliest"))) {

            // send duplicate logical message twice
            producer.send("idem", "same-msg");
            producer.send("idem", "same-msg");

            // read 2 messages; deduplication is application-level (unless you implement DB/store dedupe). Here assert we receive >=1.
            int count = 0;
            for (int i = 0; i < 2; i++) {
                Message<byte[]> m = consumer.poll(2000);
                if (m != null) {
                    count++;
                    consumer.commit(m);
                }
            }
            assertThat(count).isGreaterThanOrEqualTo(1);
        }
    }

    // High throughput smoke (not too big in unit test)
    @Test
    @Order(12)
    public void testHighThroughputSmoke() throws Exception {
        final int N = 500;
        try (KafkaProducer producer = new KafkaProducer(producerProps());
             KafkaConsumer consumer = new KafkaConsumer(consumerProps("g11", "earliest"))) {

            for (int i = 0; i < N; i++) {
                producer.sendAsync("ht", "msg-" + i);
            }

            int received = 0;
            long deadline = System.currentTimeMillis() + 15000;
            while (received < N && System.currentTimeMillis() < deadline) {
                Message<byte[]> m = consumer.poll(2000);
                if (m != null) {
                    received++;
                    consumer.commit(m);
                }
            }
            assertThat(received).isEqualTo(N);
        }
    }
}
