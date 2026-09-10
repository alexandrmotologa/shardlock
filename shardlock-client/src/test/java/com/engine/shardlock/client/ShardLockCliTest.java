package com.engine.shardlock.client;

import com.engine.shardlock.client.cli.ShardLockCli;
import com.engine.shardlock.client.sample.PostgresFencingGuardSample;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ShardLock CLI and SQL Fencing Guard Verification")
class ShardLockCliTest {

    @Test
    @DisplayName("PostgreSQL fencing token guard rejects stale writes")
    void postgresFencingGuardRejectsStaleWrites() {
        PostgresFencingGuardSample.MockPostgresDatabase db = new PostgresFencingGuardSample.MockPostgresDatabase();

        // Worker 1 writes with Token #10
        boolean firstWrite = db.updateWithFencingToken("part-0", "Initial payload", 10);
        assertTrue(firstWrite);
        assertEquals(10, db.get("part-0").fencingToken());

        // Worker 2 writes with Token #11
        boolean secondWrite = db.updateWithFencingToken("part-0", "Updated payload", 11);
        assertTrue(secondWrite);
        assertEquals(11, db.get("part-0").fencingToken());

        // Zombie Worker 1 (after a GC pause or network delay) tries to write with old Token #10
        boolean zombieWrite = db.updateWithFencingToken("part-0", "Zombie corrupted payload", 10);
        assertFalse(zombieWrite, "Database must reject stale write where client token <= current token");
        assertEquals("Updated payload", db.get("part-0").payload(), "Data must remain uncorrupted");
    }
}
