package com.evolve.integration.kafka.advanced;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaProducer<K,V> implements AutoCloseable, Closeable {
        private final PulsarClient client;
        private final Producer<byte[]> producer;
        private final Serializer<K> keySer; private final Serializer<V> valSer;
        private final boolean retriesEnabled; private final int maxRetries; private final long retryBackoffMs;
        private final boolean idempotence; private final AtomicLong seq = new AtomicLong(0);
        private final boolean transactionsEnabled; private Transaction currentTxn;
        private final long txnTimeoutMs;
        private final ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor();

        // Inside KafkaProducer
        public KafkaProducer(Properties props) throws PulsarClientException {
            String keySerName = props.getProperty("key.serializer", "string").toLowerCase(Locale.ROOT);
            String valSerName = props.getProperty("value.serializer", "string").toLowerCase(Locale.ROOT);

            if (keySerName.contains("byte")) this.keySer = (Serializer<K>) new BytesSerializer();
            else this.keySer = (Serializer<K>) new StringSerializer();

            if (valSerName.contains("byte")) this.valSer = (Serializer<V>) new BytesSerializer();
            else this.valSer = (Serializer<V>) new StringSerializer();

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

