package com.engine.shardlock.integration;

import com.engine.shardlock.application.service.RaftConsensusEngine;
import com.engine.shardlock.domain.model.*;
import com.engine.shardlock.domain.port.ClockPort;
import com.engine.shardlock.domain.state.StateMachineResult;
import com.engine.shardlock.infrastructure.storage.wal.FileChannelWalStorage;
import com.engine.shardlock.infrastructure.transport.nio.AsyncTcpTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RealTcpRaftClusterIntegrationTest {

    private final List<AsyncTcpTransport> transports = new ArrayList<>();
    private final List<FileChannelWalStorage> storages = new ArrayList<>();
    private final List<RaftConsensusEngine> engines = new ArrayList<>();

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    @AfterEach
    void tearDown() {
        for (AsyncTcpTransport t : transports) {
            t.close();
        }
        for (FileChannelWalStorage s : storages) {
            s.close();
        }
    }

    @Test
    @DisplayName("3 nodes elect leader and replicate lock acquisition over real TCP sockets")
    void testRealTcpClusterConsensus(@TempDir Path tempDir) throws Exception {
        NodeId n1 = NodeId.of("node-1");
        NodeId n2 = NodeId.of("node-2");
        NodeId n3 = NodeId.of("node-3");

        int p1 = findFreePort();
        int p2 = findFreePort();
        int p3 = findFreePort();

        Map<NodeId, InetSocketAddress> clusterMap = Map.of(
                n1, new InetSocketAddress("127.0.0.1", p1),
                n2, new InetSocketAddress("127.0.0.1", p2),
                n3, new InetSocketAddress("127.0.0.1", p3)
        );

        AsyncTcpTransport t1 = new AsyncTcpTransport(n1, p1, clusterMap);
        AsyncTcpTransport t2 = new AsyncTcpTransport(n2, p2, clusterMap);
        AsyncTcpTransport t3 = new AsyncTcpTransport(n3, p3, clusterMap);
        transports.addAll(List.of(t1, t2, t3));

        FileChannelWalStorage s1 = new FileChannelWalStorage(tempDir.resolve("node-1"));
        FileChannelWalStorage s2 = new FileChannelWalStorage(tempDir.resolve("node-2"));
        FileChannelWalStorage s3 = new FileChannelWalStorage(tempDir.resolve("node-3"));
        storages.addAll(List.of(s1, s2, s3));

        RaftConsensusEngine e1 = new RaftConsensusEngine(n1, Set.of(n2, n3), s1, t1, ClockPort.system(), null);
        RaftConsensusEngine e2 = new RaftConsensusEngine(n2, Set.of(n1, n3), s2, t2, ClockPort.system(), null);
        RaftConsensusEngine e3 = new RaftConsensusEngine(n3, Set.of(n1, n2), s3, t3, ClockPort.system(), null);
        engines.addAll(List.of(e1, e2, e3));

        // Node 1 starts election
        e1.startElection();

        // Await leader election
        long deadline = System.currentTimeMillis() + 5000;
        while (e1.role() != NodeRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertThat(e1.role()).isEqualTo(NodeRole.LEADER);
        assertThat(e1.currentTerm().value()).isEqualTo(1);

        // Leader proposes lock acquisition across the cluster
        LockCommand cmd = LockCommand.acquire("orders-p0", "worker-1", 10000, System.currentTimeMillis());
        CompletableFuture<StateMachineResult> future = e1.propose(cmd);

        StateMachineResult result = future.get(5, TimeUnit.SECONDS);
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.fencingToken()).isEqualTo(FencingToken.of(1));

        // Verify leader state machine has the lock
        assertThat(e1.stateMachine().getLock("orders-p0")).isPresent();
        assertThat(e1.stateMachine().getLock("orders-p0").get().ownerClientId()).isEqualTo("worker-1");
    }
}
