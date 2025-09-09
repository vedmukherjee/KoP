package com.evolve.integration.kafka.advanced;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaConsumer<K,V> implements AutoCloseable, Closeable {
    private final PulsarClient client;
    private final Consumer<byte[]> consumer;
    private final Deserializer<K> keyDes; private final Deserializer<V> valDes;
    private final boolean readCommitted;
    private final boolean manualAck;
    private final Duration pollInterval;
    // simple pause/resume
    private volatile boolean paused = false;

    public KafkaConsumer(Properties props) throws PulsarClientException {
        String keyDesName = props.getProperty("key.deserializer", "string").toLowerCase(Locale.ROOT);
        String valDesName = props.getProperty("value.deserializer", "string").toLowerCase(Locale.ROOT);

        if (keyDesName.contains("byte")) this.keyDes = (Deserializer<K>) new BytesDeserializer();
        else this.keyDes = (Deserializer<K>) new StringDeserializer();

        if (valDesName.contains("byte")) this.valDes = (Deserializer<V>) new BytesDeserializer();
        else this.valDes = (Deserializer<V>) new StringDeserializer();

        final String serviceUrl = props.getProperty("bootstrap.servers", "pulsar://localhost:6650");
        final String topic = Objects.requireNonNull(props.getProperty("topic"), "topic is required");
        final String groupId = props.getProperty("group.id", "default-group");
        final String reset = props.getProperty("auto.offset.reset", "earliest");
        this.readCommitted = "read_committed".equalsIgnoreCase(props.getProperty("isolation.level", "read_committed"));
        this.manualAck = !Boolean.parseBoolean(props.getProperty("enable.auto.commit", "false")); // default manual
        long pollMs = Long.parseLong(props.getProperty("poll.ms", "100"));
        this.pollInterval = Duration.ofMillis(pollMs);

        this.client = PulsarClient.builder().serviceUrl(serviceUrl).build();

        ConsumerBuilder<byte[]> cb = client.newConsumer()
                .topic(topic)
                .subscriptionName(groupId)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition("earliest".equalsIgnoreCase(reset)
                        ? SubscriptionInitialPosition.Earliest
                        : SubscriptionInitialPosition.Latest);

        this.consumer = cb.subscribe();
    }
    public KafkaConsumer(Properties props, Deserializer<K> keyDes, Deserializer<V> valDes) throws PulsarClientException {
        this.keyDes = keyDes; this.valDes = valDes;
        final String serviceUrl = props.getProperty("bootstrap.servers", "pulsar://localhost:6650");
        final String topic = Objects.requireNonNull(props.getProperty("topic"), "topic is required");
        final String groupId = props.getProperty("group.id", "default-group");
        final String reset = props.getProperty("auto.offset.reset", "earliest");
        this.readCommitted = "read_committed".equalsIgnoreCase(props.getProperty("isolation.level", "read_committed"));
        this.manualAck = !Boolean.parseBoolean(props.getProperty("enable.auto.commit", "false")); // default manual
        long pollMs = Long.parseLong(props.getProperty("poll.ms", "100"));
        this.pollInterval = Duration.ofMillis(pollMs);

        this.client = PulsarClient.builder().serviceUrl(serviceUrl).build();

        ConsumerBuilder<byte[]> cb = client.newConsumer()
                .topic(topic)
                .subscriptionName(groupId)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition("earliest".equalsIgnoreCase(reset)
                        ? SubscriptionInitialPosition.Earliest
                        : SubscriptionInitialPosition.Latest);

        // Pulsar shows only committed txn messages by default; no extra flag needed.
        // We keep readCommitted to mirror Kafka API.

        this.consumer = cb.subscribe();
    }

    /** poll: returns a batch of records within timeout */
    public ConsumerRecords<K,V> poll(Duration timeout) throws PulsarClientException {
        if (paused) return new ConsumerRecords<>(Collections.emptyList());
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout.toMillis());
        List<ConsumerRecord<K,V>> out = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            // receive with timeout (ms)
            int waitMs = (int) Math.max(1, pollInterval.toMillis());
            Message<byte[]> msg;
            try {
                msg = consumer.receive(waitMs, TimeUnit.MILLISECONDS);
            } catch (PulsarClientException e) {
                // treat as no-message
                break;
            }
            if (msg == null) break;
            String key = msg.getKey();
            K k = keyDes.deserialize(key==null?null:key.getBytes(StandardCharsets.ISO_8859_1));
            V v = valDes.deserialize(msg.getValue());
            out.add(new ConsumerRecord<>(msg.getTopicName(), key, k, v, msg.getMessageId(), msg.getPublishTime(), msg.getProperties()));
            if (!manualAck) {
                consumer.acknowledgeAsync(msg);
            }
        }
        return new ConsumerRecords<>(out);
    }

    /** commitSync: ack all messages in the provided records */
    public void commitSync(ConsumerRecords<K,V> records) throws PulsarClientException {
        for (ConsumerRecord<K,V> r : records) {
            consumer.acknowledge(r.messageId);
        }
    }

    /** commitAsync: ack asynchronously */
    public CompletableFuture<Void> commitAsync(ConsumerRecords<K,V> records) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (ConsumerRecord<K,V> r : records) {
            futures.add(consumer.acknowledgeAsync(r.messageId));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    public void pause() { paused = true; }
    public void resume() { paused = false; }

    /** seek to earliest/latest */
    public void seekToBeginning() throws PulsarClientException { consumer.seek(MessageId.earliest); }
    public void seekToEnd() throws PulsarClientException { consumer.seek(MessageId.latest); }

    /** assign/subscribe compatibility (no-ops in this simplified wrapper) */
    public void subscribe(Collection<String> topics) { /* single-topic wrapper; extend as needed */ }
    public void assign(Collection<String> tps) { /* Pulsar routing differs; kept for API parity */ }

    @Override public void close() {
        try { consumer.close(); } catch (Exception ignore) {}
        try { client.close(); } catch (Exception ignore) {}
    }
}

