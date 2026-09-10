package com.engine.shardlock.simulation;

import com.engine.shardlock.application.service.RaftConsensusEngine;
import com.engine.shardlock.domain.model.LogIndex;
import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.model.NodeRole;
import com.engine.shardlock.domain.model.Term;
import com.engine.shardlock.domain.port.ClockPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReadIndex Linearizable Read Verification")
class ReadIndexTest {

    private InMemoryNetworkSimulator network;
    private final NodeId node1 = NodeId.of("node-1");
    private final NodeId node2 = NodeId.of("node-2");
    private final NodeId node3 = NodeId.of("node-3");

    private RaftConsensusEngine engine1;
    private RaftConsensusEngine engine2;
    private RaftConsensusEngine engine3;

    private final AtomicLong simulatedTime = new AtomicLong(1000);
    private final ClockPort clock = simulatedTime::get;

    @BeforeEach
    void setUp() {
        network = new InMemoryNetworkSimulator();

        engine1 = new RaftConsensusEngine(node1, Set.of(node2, node3), new InMemoryStoragePort(), network.createTransportFor(node1), clock, null);
        engine2 = new RaftConsensusEngine(node2, Set.of(node1, node3), new InMemoryStoragePort(), network.createTransportFor(node2), clock, null);
        engine3 = new RaftConsensusEngine(node3, Set.of(node1, node2), new InMemoryStoragePort(), network.createTransportFor(node3), clock, null);

        engine1.startElection();
        assertEquals(NodeRole.LEADER, engine1.role());
    }

    @Test
    @DisplayName("Leader executes linearizable ReadIndex through quorum confirmation without writing to log")
    void leaderExecutesReadIndexWithoutLogWrite() throws Exception {
        LogIndex logIndexBefore = engine1.raftLog().lastIndex();

        CompletableFuture<Void> readFuture = engine1.readIndex();
        readFuture.get(1, TimeUnit.SECONDS);

        assertTrue(readFuture.isDone());
        assertFalse(readFuture.isCompletedExceptionally());

        // Zero disk/log write overhead
        assertEquals(logIndexBefore, engine1.raftLog().lastIndex(), "ReadIndex must not append any new log entry");
    }

    @Test
    @DisplayName("Follower rejects ReadIndex call with NotLeaderException")
    void followerRejectsReadIndex() {
        CompletableFuture<Void> readFuture = engine2.readIndex();

        ExecutionException ex = assertThrows(ExecutionException.class, () -> readFuture.get(1, TimeUnit.SECONDS));
        assertInstanceOf(RaftConsensusEngine.NotLeaderException.class, ex.getCause());
    }
}
