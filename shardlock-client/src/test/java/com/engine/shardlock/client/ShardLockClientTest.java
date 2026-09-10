package com.engine.shardlock.client;

import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.model.NodeRole;
import com.engine.shardlock.infrastructure.server.ShardLockServerNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShardLockClientTest {

    private ShardLockServerNode serverNode;
    private int httpPort;
    private int peerPort;
    private ShardLockClient client;

    private int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        httpPort = findFreePort();
        peerPort = findFreePort();

        serverNode = new ShardLockServerNode(
                NodeId.of("client-test-node"),
                httpPort,
                peerPort,
                Map.of(),
                Map.of(),
                tempDir
        );
        serverNode.start();

        // Wait for leader election
        long deadline = System.currentTimeMillis() + 3000;
        while (serverNode.consensusEngine().role() != NodeRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        client = ShardLockClient.builder()
                .endpoint("http://127.0.0.1:" + httpPort)
                .clientId("test-worker")
                .requestTimeout(Duration.ofSeconds(3))
                .build();
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (serverNode != null) {
            serverNode.stop();
        }
    }

    @Test
    @DisplayName("Acquires and releases partition lock cleanly")
    void testAcquireAndRelease() {
        Optional<LockHandle> handleOpt = client.acquire("partition-orders-0", Duration.ofSeconds(5));
        assertThat(handleOpt).isPresent();

        LockHandle handle = handleOpt.get();
        assertThat(handle.resource()).isEqualTo("partition-orders-0");
        assertThat(handle.clientId()).isEqualTo("test-worker");
        assertThat(handle.fencingToken()).isGreaterThanOrEqualTo(1);

        boolean released = client.release(handle);
        assertThat(released).isTrue();
    }

    @Test
    @DisplayName("Lock contention between workers prevents concurrent access")
    void testLockContention() {
        ShardLockClient workerA = ShardLockClient.builder()
                .endpoint("http://127.0.0.1:" + httpPort)
                .clientId("worker-a")
                .build();

        ShardLockClient workerB = ShardLockClient.builder()
                .endpoint("http://127.0.0.1:" + httpPort)
                .clientId("worker-b")
                .build();

        Optional<LockHandle> lockA = workerA.acquire("partition-shared", Duration.ofSeconds(10));
        assertThat(lockA).isPresent();

        // Worker B fails to acquire because Worker A holds it
        Optional<LockHandle> lockB = workerB.acquire("partition-shared", Duration.ofSeconds(10));
        assertThat(lockB).isEmpty();

        // Worker A releases
        workerA.release(lockA.get());

        // Now Worker B succeeds
        Optional<LockHandle> lockBAfter = workerB.acquire("partition-shared", Duration.ofSeconds(10));
        assertThat(lockBAfter).isPresent();
        assertThat(lockBAfter.get().fencingToken()).isGreaterThan(lockA.get().fencingToken());

        workerB.release(lockBAfter.get());
    }

    @Test
    @DisplayName("tryWithLock automatically maintains heartbeat and releases on block completion")
    void testTryWithLock() {
        AtomicBoolean executed = new AtomicBoolean(false);

        boolean result = client.tryWithLock("partition-auto", Duration.ofSeconds(1), handle -> {
            executed.set(true);
            assertThat(handle.fencingToken()).isGreaterThanOrEqualTo(1);
            // Verify lock is recorded as held
            assertThat(serverNode.lockManager().getLock("partition-auto")).isPresent();
        });

        assertThat(result).isTrue();
        assertThat(executed).isTrue();

        // Verify lock is released after block finishes
        assertThat(serverNode.lockManager().getLock("partition-auto")).isEmpty();
    }

    @Test
    @DisplayName("FencingTokenGuard prevents zombie writes from lagged workers")
    void testFencingTokenGuardSplitBrainProtection() {
        FencingTokenGuard storageGuard = new FencingTokenGuard();

        // Worker A acquired Token 101 earlier
        long workerAToken = 101;

        // Worker B acquired Token 102 after Worker A paused
        long workerBToken = 102;

        AtomicBoolean workerBWrote = new AtomicBoolean(false);
        storageGuard.executeWithToken("database-row-1", workerBToken, () -> {
            workerBWrote.set(true);
        });

        assertThat(workerBWrote).isTrue();
        assertThat(storageGuard.getHighestToken("database-row-1")).isEqualTo(102);

        // Worker A wakes up and attempts write with Token 101 (< 102)
        assertThatThrownBy(() -> {
            storageGuard.executeWithToken("database-row-1", workerAToken, () -> {
                // Must not be executed!
            });
        }).isInstanceOf(FencingTokenGuard.StaleFencingTokenException.class)
          .hasMessageContaining("Rejected stale write on resource 'database-row-1'");
    }
}
