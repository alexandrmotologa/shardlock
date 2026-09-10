package com.engine.shardlock.simulation;

import com.engine.shardlock.application.service.RaftConsensusEngine;
import com.engine.shardlock.domain.model.FencingToken;
import com.engine.shardlock.domain.model.LockCommand;
import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.model.NodeRole;
import com.engine.shardlock.domain.port.ClockPort;
import com.engine.shardlock.domain.state.StateMachineResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RaftConsensusEngineSimulationTest {

    private InMemoryNetworkSimulator network;
    private NodeId n1;
    private NodeId n2;
    private NodeId n3;

    private RaftConsensusEngine node1;
    private RaftConsensusEngine node2;
    private RaftConsensusEngine node3;

    @BeforeEach
    void setUp() {
        network = new InMemoryNetworkSimulator();
        n1 = NodeId.of("node-1");
        n2 = NodeId.of("node-2");
        n3 = NodeId.of("node-3");

        node1 = createEngine(n1, Set.of(n2, n3));
        node2 = createEngine(n2, Set.of(n1, n3));
        node3 = createEngine(n3, Set.of(n1, n2));
    }

    private RaftConsensusEngine createEngine(NodeId id, Set<NodeId> peers) {
        return new RaftConsensusEngine(
                id,
                peers,
                new InMemoryStoragePort(),
                network.createTransportFor(id),
                ClockPort.system(),
                null
        );
    }

    @Test
    @DisplayName("Node 1 starts election, collects votes, and becomes LEADER")
    void testLeaderElection() {
        assertThat(node1.role()).isEqualTo(NodeRole.FOLLOWER);
        assertThat(node2.role()).isEqualTo(NodeRole.FOLLOWER);
        assertThat(node3.role()).isEqualTo(NodeRole.FOLLOWER);

        node1.startElection();

        assertThat(node1.role()).isEqualTo(NodeRole.LEADER);
        assertThat(node1.currentTerm().value()).isEqualTo(1);
        assertThat(node1.currentLeaderId()).isEqualTo(n1);

        // Followers recognize Node 1 as leader once heartbeat arrives
        node1.broadcastHeartbeats();
        assertThat(node2.currentLeaderId()).isEqualTo(n1);
        assertThat(node3.currentLeaderId()).isEqualTo(n1);
    }

    @Test
    @DisplayName("Leader proposes lock acquisition, replicates to quorum, and issues FencingToken 1")
    void testProposeLockAcquisition() throws Exception {
        node1.startElection();
        assertThat(node1.role()).isEqualTo(NodeRole.LEADER);

        LockCommand acquireCmd = LockCommand.acquire("orders-0", "worker-a", 10000L, System.currentTimeMillis());
        CompletableFuture<StateMachineResult> future = node1.propose(acquireCmd);

        // Under synchronous simulator, quorum is reached during propose or immediate broadcast
        node1.broadcastHeartbeats();

        StateMachineResult result = future.get();
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.fencingToken()).isEqualTo(FencingToken.of(1));
        assertThat(node1.stateMachine().getLock("orders-0")).isPresent();
        assertThat(node1.stateMachine().getLock("orders-0").get().ownerClientId()).isEqualTo("worker-a");
    }

    @Test
    @DisplayName("Proposing command on a follower node throws NotLeaderException")
    void testProposeOnFollowerThrows() {
        node1.startElection();
        node1.broadcastHeartbeats();

        LockCommand acquireCmd = LockCommand.acquire("orders-0", "worker-a", 10000L, System.currentTimeMillis());
        CompletableFuture<StateMachineResult> future = node2.propose(acquireCmd);

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RaftConsensusEngine.NotLeaderException.class);
    }

    @Test
    @DisplayName("Subsequent lock acquisitions receive strictly monotonic fencing tokens")
    void testMonotonicFencingTokens() throws Exception {
        node1.startElection();

        long now = System.currentTimeMillis();
        CompletableFuture<StateMachineResult> f1 = node1.propose(LockCommand.acquire("res-1", "worker-a", 5000, now));
        node1.broadcastHeartbeats();
        StateMachineResult r1 = f1.get();

        CompletableFuture<StateMachineResult> f2 = node1.propose(LockCommand.acquire("res-2", "worker-b", 5000, now));
        node1.broadcastHeartbeats();
        StateMachineResult r2 = f2.get();

        assertThat(r1.fencingToken().value()).isEqualTo(1);
        assertThat(r2.fencingToken().value()).isEqualTo(2);
        assertThat(r2.fencingToken().isGreaterThan(r1.fencingToken())).isTrue();
    }
}
