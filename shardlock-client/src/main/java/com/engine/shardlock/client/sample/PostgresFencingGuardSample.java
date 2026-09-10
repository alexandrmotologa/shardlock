package com.engine.shardlock.client.sample;

import com.engine.shardlock.client.LockHandle;
import com.engine.shardlock.client.ShardLockClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Concrete sample demonstrating how downstream database writes (e.g. PostgreSQL, MySQL)
 * integrate monotonic fencing tokens to prevent zombie/stale split-brain overwrites
 * (Martin Kleppmann's GC pause dilemma).
 */
public class PostgresFencingGuardSample {

    private static final Logger log = LoggerFactory.getLogger(PostgresFencingGuardSample.class);

    /**
     * Simulated PostgreSQL storage table:
     * <pre>
     * CREATE TABLE partition_state (
     *     partition_id   VARCHAR(64) PRIMARY KEY,
     *     payload        TEXT NOT NULL,
     *     fencing_token  BIGINT NOT NULL
     * );
     * </pre>
     */
    public static class MockPostgresDatabase {
        public record Row(String partitionId, String payload, long fencingToken) {}

        private final ConcurrentHashMap<String, Row> table = new ConcurrentHashMap<>();

        public synchronized boolean updateWithFencingToken(String partitionId, String newPayload, long clientToken) {
            Row current = table.get(partitionId);
            if (current != null && clientToken <= current.fencingToken()) {
                // Stale write detected: client token is older than or equal to current token!
                // SQL: UPDATE partition_state SET payload = ?, fencing_token = ?
                //      WHERE partition_id = ? AND fencing_token < ?;
                log.warn("DATABASE REJECTED STALE WRITE: Current token is #{}, but client submitted #{}",
                        current.fencingToken(), clientToken);
                return false;
            }

            table.put(partitionId, new Row(partitionId, newPayload, clientToken));
            log.info("DATABASE ACCEPTED WRITE for '{}': Payload='{}', Token=#{}",
                    partitionId, newPayload, clientToken);
            return true;
        }

        public Row get(String partitionId) {
            return table.get(partitionId);
        }
    }

    public static void main(String[] args) {
        MockPostgresDatabase db = new MockPostgresDatabase();

        try (ShardLockClient client = ShardLockClient.builder()
                .endpoint("http://localhost:8001")
                .clientId("primary-worker")
                .build()) {

            String partition = "orders-partition-42";

            // 1. Primary worker acquires lease
            Optional<LockHandle> leaseOpt = client.acquire(partition, Duration.ofSeconds(10));
            if (leaseOpt.isEmpty()) {
                log.error("Could not acquire lease for partition");
                return;
            }

            LockHandle handle = leaseOpt.get();
            log.info("Acquired lease with fencing token: #{}", handle.fencingToken());

            // 2. Perform safe database write guarded by token
            boolean written = db.updateWithFencingToken(partition, "{ orderId: 101, status: 'PROCESSED' }", handle.fencingToken());
            log.info("Database write status: {}", written);

            // 3. Simulating Martin Kleppmann's GC pause scenario:
            // An old zombie worker wakes up with a stale token (#(current - 1))
            long staleToken = Math.max(1, handle.fencingToken() - 1);
            log.info("Simulating zombie worker waking up with stale token #{}...", staleToken);
            boolean zombieResult = db.updateWithFencingToken(partition, "{ orderId: 999, status: 'CORRUPTED' }", staleToken);

            if (!zombieResult) {
                log.info("SUCCESS: Fencing token prevented zombie write from corrupting database!");
            }
        }
    }
}
