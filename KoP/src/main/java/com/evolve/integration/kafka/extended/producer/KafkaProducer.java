package com.evolve.integration.kafka.extended.producer;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;

public class KafkaProducer implements AutoCloseable {

    private final PulsarClient client;
    private final Producer<byte[]> producer;
    private Transaction txn;
    private final boolean idempotenceEnabled;

    public KafkaProducer(Properties props) throws PulsarClientException {
        String serviceUrl = props.getProperty("bootstrap.servers", "pulsar://localhost:6650");
        String topic = props.getProperty("topic");
        this.idempotenceEnabled = Boolean.parseBoolean(props.getProperty("enable.idempotence", "false"));

        this.client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        ProducerBuilder<byte[]> builder = client.newProducer()
                .topic(topic)
                .blockIfQueueFull(true)
                .sendTimeout(0, TimeUnit.SECONDS); // disable client-side send timeout

        // compression mapping
        String compression = props.getProperty("compression.type", "NONE").toUpperCase();
        switch (compression) {
            case "LZ4": builder.compressionType(CompressionType.LZ4); break;
            case "ZLIB": builder.compressionType(CompressionType.ZLIB); break;
            case "ZSTD": builder.compressionType(CompressionType.ZSTD); break;
            case "SNAPPY": builder.compressionType(CompressionType.SNAPPY); break;
            default: builder.compressionType(CompressionType.NONE);
        }

        // batching
        if (props.containsKey("batch.size")) {
            builder.batchingMaxMessages(Integer.parseInt(props.getProperty("batch.size")));
        }
        if (props.containsKey("linger.ms")) {
            builder.batchingMaxPublishDelay(Long.parseLong(props.getProperty("linger.ms")), TimeUnit.MILLISECONDS);
        }

        this.producer = builder.create();
    }

    // Synchronous send (blocks until acknowledged)
    public MessageId send(String key, String value) throws PulsarClientException {
        TypedMessageBuilder<byte[]> mb = producer.newMessage()
                .key(key)
                .value(value.getBytes());

        /*if (txn != null) {
            mb.transaction(txn);
        }*/

        if (idempotenceEnabled) {
            // Use a high-resolution sequence id for app-level dedup; Pulsar sequenceId expects long
            mb.sequenceId(System.nanoTime());
        }

        return mb.send();
    }

    // Asynchronous send
    public CompletableFuture<MessageId> sendAsync(String key, String value) {
        TypedMessageBuilder<byte[]> mb = producer.newMessage()
                .key(key)
                .value(value.getBytes());

        /*if (txn != null) {
            mb.transaction(txn);
        }*/
        if (idempotenceEnabled) {
            mb.sequenceId(System.nanoTime());
        }

        return mb.sendAsync();
    }

    // Transaction APIs
    public void beginTransaction(long timeout, TimeUnit unit) {
        txn = client.newTransaction()
                .withTransactionTimeout(timeout, unit)
                .build()
                .join();
    }

    public void commitTransaction() {
        if (txn != null) {
            txn.commit().join();
            txn = null;
        }
    }

    public void abortTransaction() {
        if (txn != null) {
            txn.abort().join();
            txn = null;
        }
    }

    public boolean inTransaction() {
        return txn != null;
    }

    @Override
    public void close() throws PulsarClientException {
        if (producer != null) producer.close();
        if (client != null) client.close();
    }
}
