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

    @Test
    @DisplayName("ShardLock CLI prints version with --version, -v, and version")
    void cliPrintsVersion() {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        java.io.PrintStream originalOut = System.out;
        try {
            System.setOut(new java.io.PrintStream(out));
            ShardLockCli.main(new String[]{"--version"});
            assertTrue(out.toString().contains("ShardLock version 1.0.0"));

            out.reset();
            ShardLockCli.main(new String[]{"-v"});
            assertTrue(out.toString().contains("ShardLock version 1.0.0"));

            out.reset();
            ShardLockCli.main(new String[]{"version"});
            assertTrue(out.toString().contains("ShardLock version 1.0.0"));
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    @DisplayName("ShardLock CLI prints help with --help and -h")
    void cliPrintsHelp() {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        java.io.PrintStream originalOut = System.out;
        try {
            System.setOut(new java.io.PrintStream(out));
            ShardLockCli.main(new String[]{"--help"});
            assertTrue(out.toString().contains("=== ShardLock CLI Usage ==="));

            out.reset();
            ShardLockCli.main(new String[]{"-h"});
            assertTrue(out.toString().contains("=== ShardLock CLI Usage ==="));
        } finally {
            System.setOut(originalOut);
        }
    }
}
