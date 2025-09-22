package com.evolve.integration.kafka.kop;

public class TransactionCoordinator {
    // Stub for transaction coordinator. Real KoP maps transactional metadata to Pulsar topics.
    public void beginTransaction(String txId) {}
    public void commit(String txId) {}
    public void abort(String txId) {}
}
