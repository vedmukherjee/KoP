package com.evolve.integration.kafka.advanced;

// File: io/example/kafkapulsar/KafkaLikeWrappers.java
// Purpose: Provide Kafka-like Producer/Consumer APIs backed by Apache Pulsar (NO KoP)
// Notes:
//  - Requires Pulsar 2.10+ (transactions require broker & namespace features enabled)
//  - This is a self-contained demo-style implementation. In real projects, split into files.
//  - Add Maven deps:
//      <dependency>
//        <groupId>org.apache.pulsar</groupId>
//        <artifactId>pulsar-client</artifactId>
//        <version>3.2.3</version>
//      </dependency>
//
//  - Namespace/broker MUST have transactions & (optionally) dedup enabled for full parity:
//      bin/pulsar-admin namespaces set-deduplication --enable public/default
//      bin/pulsar-admin namespaces set-is-allow-auto-update-schema true public/default
//      broker.conf: systemTopicEnabled=true, transactionCoordinatorEnabled=true
//


import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;



/** =============================
 *  Kafka-like Producer (Pulsar)
 *  ============================= */
public class KafkaLikeWrappers<K,V> {

    public static class KafkaProducer<K,V> implements AutoCloseable, Closeable {
        private final PulsarClient client;
        private final Producer<byte[]> producer;
        private final Serializer<K> keySer; private final Serializer<V> valSer;
        private final boolean retriesEnabled; private final int maxRetries; private final long retryBackoffMs;
        private final boolean idempotence; private final AtomicLong seq = new AtomicLong(0);
        private final boolean transactionsEnabled; private Transaction currentTxn;
        private final long txnTimeoutMs;
        private final ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor();

        public KafkaProducer(Properties props, Serializer<K> keySer, Serializer<V> valSer) throws PulsarClientException {
            this.keySer = keySer; this.valSer = valSer;

            final String serviceUrl = props.getProperty("bootstrap.servers", "pulsar://localhost:6650");
            final String topic = Objects.requireNonNull(props.getProperty("topic"), "topic is required");
            final boolean batching = Boolean.parseBoolean(props.getProperty("enable.batching", "true"));
            final int batchSize = Integer.parseInt(props.getProperty("batch.size", "1000"));
            final long lingerMs = Long.parseLong(props.getProperty("linger.ms", "1"));
            final String compression = props.getProperty("compression.type", "NONE");
            final boolean blockIfQueueFull = Boolean.parseBoolean(props.getProperty("enable.blocking", "true"));
            final int maxPending = Integer.parseInt(props.getProperty("max.in.flight.requests.per.connection", "1000"));
            this.retriesEnabled = Boolean.parseBoolean(props.getProperty("retries.enable", "true"));
            this.maxRetries = Integer.parseInt(props.getProperty("retries", "5"));
            this.retryBackoffMs = Long.parseLong(props.getProperty("retry.backoff.ms", "100"));
            this.idempotence = Boolean.parseBoolean(props.getProperty("enable.idempotence", "true"));
            this.transactionsEnabled = Boolean.parseBoolean(props.getProperty("enable.transactions", "false"));
            this.txnTimeoutMs = Long.parseLong(props.getProperty("transaction.timeout.ms", "60000"));

            this.client = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .build();

            ProducerBuilder<byte[]> builder = client.newProducer()
                    .topic(topic)
                    .enableBatching(batching)
                    .batchingMaxMessages(batchSize)
                    .batchingMaxPublishDelay(lingerMs, TimeUnit.MILLISECONDS)
                    .compressionType(Maps.compression(compression))
                    .blockIfQueueFull(blockIfQueueFull)
                    .maxPendingMessages(maxPending)
                    .sendTimeout(0, TimeUnit.SECONDS); // let retries control timeouts

            // For broker-level dedup to work best, set a stable producer name when idempotence enabled
            if (idempotence) {
                String pname = props.getProperty("producer.name", "kafka-like-producer");
                builder.producerName(pname);
            }

            this.producer = builder.create();
        }

        /** Initialize transactions (Kafka parity). */
        public void initTransactions() { /* noop for Pulsar; ensure broker is configured */ }

        /** Begin a transaction */
        public synchronized void beginTransaction() throws PulsarClientException {
            if (!transactionsEnabled) throw new IllegalStateException("Transactions not enabled. Set enable.transactions=true");
            if (currentTxn != null) throw new IllegalStateException("Transaction already in progress");
            this.currentTxn = this.client.newTransaction().withTransactionTimeout(txnTimeoutMs, TimeUnit.MILLISECONDS).build().join();
        }

        /** Commit a transaction */
        public synchronized void commitTransaction() {
            if (currentTxn == null) throw new IllegalStateException("No active transaction");
            currentTxn.commit().join();
            currentTxn = null;
        }

        /** Abort a transaction */
        public synchronized void abortTransaction() {
            if (currentTxn == null) throw new IllegalStateException("No active transaction");
            currentTxn.abort().join();
            currentTxn = null;
        }

        /** Build and send a message (returns CompletableFuture<RecordMetadata>) with retries. */
        public CompletableFuture<RecordMetadata> send(final ProducerRecord<K,V> rec) {
            return sendWithAttempt(rec, 0);
        }

        private CompletableFuture<RecordMetadata> sendWithAttempt(final ProducerRecord<K,V> rec, final int attempt) {
            CompletableFuture<RecordMetadata> result = new CompletableFuture<>();

            // Build the TypedMessageBuilder, optionally with txn
            TypedMessageBuilder<byte[]> tmb;
            Transaction txnSnapshot;
            synchronized (this) {
                txnSnapshot = this.currentTxn;
            }
            if (txnSnapshot != null) {
                tmb = producer.newMessage(txnSnapshot);
            } else {
                tmb = producer.newMessage();
            }

            // key and value
            if (rec.key != null) {
                byte[] kbytes = keySer.serialize(rec.key);
                // Pulsar key is string; use ISO_8859_1 to preserve bytes
                tmb.key(new String(Objects.requireNonNullElse(kbytes, new byte[0]), StandardCharsets.ISO_8859_1));
            }
            byte[] vbytes = valSer.serialize(rec.value);
            tmb.value(vbytes);

            // headers -> properties (base64)
            for (Header h : rec.headers) {
                String b64 = Base64.getEncoder().encodeToString(h.value);
                tmb.property("hdr_"+h.key, b64);
            }

            // idempotence sequence id
            if (idempotence) {
                tmb.sequenceId(seq.incrementAndGet());
            }

            // send asynchronously
            CompletableFuture<MessageId> sendFuture = tmb.sendAsync();

            sendFuture.whenComplete((msgId, sendErr) -> {
                if (sendErr == null) {
                    result.complete(new RecordMetadata(producer.getTopic(), msgId, System.currentTimeMillis()));
                } else {
                    if (!retriesEnabled || attempt >= maxRetries) {
                        result.completeExceptionally(sendErr);
                    } else {
                        long delay = retryBackoffMs * (1L << Math.min(attempt, 6));
                        retryScheduler.schedule(() -> {
                            // recursive retry
                            sendWithAttempt(rec, attempt + 1).whenComplete((rm, ex) -> {
                                if (ex == null) result.complete(rm);
                                else result.completeExceptionally(ex);
                            });
                        }, delay, TimeUnit.MILLISECONDS);
                    }
                }
            });

            return result;
        }

        /** Synchronous send for convenience. */
        public RecordMetadata sendSync(ProducerRecord<K,V> rec) throws ExecutionException, InterruptedException {
            return send(rec).get();
        }

        public void flush() throws PulsarClientException { producer.flush(); }

        @Override public void close() {
            try { producer.close(); } catch (Exception ignore) {}
            try { client.close(); } catch (Exception ignore) {}
            try { retryScheduler.shutdownNow(); } catch (Exception ignore) {}
        }
    }

    /** ============================
     *  Kafka-like Consumer (Pulsar)
     *  ============================ */
    public static class KafkaConsumer<K,V> implements AutoCloseable, Closeable {
        private final PulsarClient client;
        private final Consumer<byte[]> consumer;
        private final Deserializer<K> keyDes; private final Deserializer<V> valDes;
        private final boolean readCommitted;
        private final boolean manualAck;
        private final Duration pollInterval;
        // simple pause/resume
        private volatile boolean paused = false;

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

    /* ======================
     * Usage Examples (JUnit)
     * ======================
     *
     * Properties p = new Properties();
     * p.setProperty("bootstrap.servers", "pulsar://localhost:6650");
     * p.setProperty("topic", "persistent://public/default/my-topic");
     * p.setProperty("enable.batching", "true");
     * p.setProperty("batch.size", "500");
     * p.setProperty("linger.ms", "5");
     * p.setProperty("compression.type", "LZ4");
     * p.setProperty("enable.idempotence", "true");
     * p.setProperty("retries", "10");
     * p.setProperty("retry.backoff.ms", "50");
     * p.setProperty("enable.transactions", "true");
     *
     * KafkaProducer<String,String> prod = new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
     * prod.initTransactions();
     * prod.beginTransaction();
     * prod.sendSync(new ProducerRecord<>(p.getProperty("topic"), "k1", "v1"));
     * prod.commitTransaction();
     * prod.close();
     *
     * Properties c = new Properties();
     * c.setProperty("bootstrap.servers", "pulsar://localhost:6650");
     * c.setProperty("topic", "persistent://public/default/my-topic");
     * c.setProperty("group.id", "g1");
     * c.setProperty("auto.offset.reset", "earliest");
     * c.setProperty("isolation.level", "read_committed");
     * KafkaConsumer<String,String> cons = new KafkaConsumer<>(c, new StringDeserializer(), new StringDeserializer());
     * ConsumerRecords<String,String> recs = cons.poll(Duration.ofMillis(500));
     * cons.commitSync(recs);
     * cons.close();
     */
}
