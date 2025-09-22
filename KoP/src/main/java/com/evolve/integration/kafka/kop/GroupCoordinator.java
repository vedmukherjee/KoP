package com.evolve.integration.kafka.kop;

public class GroupCoordinator {
    // Very small stub to show where group membership/offset commit logic would be.
    // KoP implements full-group-coordinator semantics (consumer group membership,
    // partitions assignments, committed offsets stored in metadata topics).
    public void handleJoinGroup(String groupId) {
        // join group logic...
    }
}
