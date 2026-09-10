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

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NetworkPartitionChaosTest {

    private InMemoryNetworkSimulator network;
    private final List<NodeId> nodeIds = new ArrayList<>();
    private final Map<NodeId, RaftConsensusEngine> engines = new HashMap<>();

    @BeforeEach
    void setUp() {
        network = new InMemoryNetworkSimulator();
        nodeIds.clear();
        engines.clear();

        for (int i = 1; i <= 5; i++) {
            nodeIds.add(NodeId.of("node-" + i));
        }

        for (NodeId id : nodeIds) {
            Set<NodeId> peers = new HashSet<>(nodeIds);
            peers.remove(id);

            RaftConsensusEngine engine = new RaftConsensusEngine(
                    id,
                    peers,
                    new InMemoryStoragePort(),
                    network.createTransportFor(id),
                    ClockPort.system(),
                    null
            );
            engines.put(id, engine);
        }
    }

    @Test
    @DisplayName("Majority partition (3/5) continues processing locks while minority partition (2/5) rejects writes")
    void testNetworkPartitionMajorityVsMinority() throws Exception {
        RaftConsensusEngine n1 = engines.get(NodeId.of("node-1"));
        RaftConsensusEngine n2 = engines.get(NodeId.of("node-2"));
        RaftConsensusEngine n3 = engines.get(NodeId.of("node-3"));
        RaftConsensusEngine n4 = engines.get(NodeId.of("node-4"));
        RaftConsensusEngine n5 = engines.get(NodeId.of("node-5"));

        // Step 1: Normal cluster election: Node 1 becomes leader
        n1.startElection();
        assertThat(n1.role()).isEqualTo(NodeRole.LEADER);
        n1.broadcastHeartbeats();

        // Step 2: Acquire initial lock before partition
        long now = System.currentTimeMillis();
        CompletableFuture<StateMachineResult> initLock = n1.propose(LockCommand.acquire("orders-0", "worker-main", 10000, now));
        n1.broadcastHeartbeats();
        assertThat(initLock.get().fencingToken()).isEqualTo(FencingToken.of(1));

        // Step 3: Inject network partition: {node-1, node-2, node-3} vs {node-4, node-5}
        List<NodeId> majority = List.of(n1.nodeId(), n2.nodeId(), n3.nodeId());
        List<NodeId> minority = List.of(n4.nodeId(), n5.nodeId());

        for (NodeId maj : majority) {
            for (NodeId min : minority) {
                network.partition(maj, min);
                network.partition(min, maj);
            }
        }

        // Step 4: Majority partition (3 nodes) can still commit locks (quorum is 3/5)
        CompletableFuture<StateMachineResult> majLock = n1.propose(LockCommand.acquire("orders-1", "worker-majority", 10000, now));
        n1.broadcastHeartbeats();
        StateMachineResult majResult = majLock.get();
        assertThat(majResult.isSuccess()).isTrue();
        assertThat(majResult.fencingToken()).isEqualTo(FencingToken.of(2));

        // Step 5: Minority partition cannot elect a leader or acquire locks
        n4.startElection();
        // n4 gets vote only from itself and n5 (2/5 < quorum 3)
        assertThat(n4.role()).isNotEqualTo(NodeRole.LEADER);
        assertThat(n4.role()).isEqualTo(NodeRole.CANDIDATE);

        // Proposals on minority partition fail
        CompletableFuture<StateMachineResult> minLock = n4.propose(LockCommand.acquire("orders-2", "worker-minority", 10000, now));
        assertThatThrownBy(minLock::get)
                .hasCauseInstanceOf(RaftConsensusEngine.NotLeaderException.class);

        // Step 6: Heal partition and reconcile logs
        network.clearAllPartitions();
        n1.startElection();
        assertThat(n1.role()).isEqualTo(NodeRole.LEADER);
        n1.broadcastHeartbeats();

        // Nodes 4 and 5 step down to FOLLOWER and catch up with majority log
        assertThat(n4.role()).isEqualTo(NodeRole.FOLLOWER);
        assertThat(n5.role()).isEqualTo(NodeRole.FOLLOWER);
        assertThat(n4.currentLeaderId()).isEqualTo(n1.nodeId());
        assertThat(n5.currentLeaderId()).isEqualTo(n1.nodeId());

        // Verify committed locks are reconciled on recovered nodes
        assertThat(n4.raftLog().commitIndex()).isEqualTo(n1.raftLog().commitIndex());
        assertThat(n5.raftLog().commitIndex()).isEqualTo(n1.raftLog().commitIndex());
        assertThat(n4.stateMachine().getLock("orders-1")).isPresent();
        assertThat(n5.stateMachine().getLock("orders-1")).isPresent();
    }
}
