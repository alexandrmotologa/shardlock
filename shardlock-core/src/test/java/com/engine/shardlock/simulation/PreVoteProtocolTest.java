package com.engine.shardlock.simulation;

import com.engine.shardlock.application.service.RaftConsensusEngine;
import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.model.NodeRole;
import com.engine.shardlock.domain.model.Term;
import com.engine.shardlock.domain.port.ClockPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Raft §9.6 Pre-Vote Protocol Simulation Verification")
class PreVoteProtocolTest {

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

        // Elect node1 as leader in Term 1
        engine1.startElection();
        assertEquals(NodeRole.LEADER, engine1.role());
        assertEquals(Term.of(1), engine1.currentTerm());
    }

    @Test
    @DisplayName("Partitioned minority node does not increment term when pre-vote is rejected")
    void partitionedNodeDoesNotDisruptClusterWithInflatedTerm() {
        // Partition node3 completely
        network.isolate(node3);

        // Advance time on node3 past election timeout
        simulatedTime.addAndGet(500);

        // node3 times out and initiates Pre-Vote
        engine3.checkElectionTimeout();

        // Under Pre-Vote, node3 entered PRE_CANDIDATE, but because it got 0 peer responses,
        // it NEVER incremented currentTerm to 2!
        assertEquals(Term.of(1), engine3.currentTerm(), "Term must not increment to 2 during speculative Pre-Vote");

        // Heal partition
        network.heal(node3);

        // Heartbeat from leader reaches node3
        engine1.broadcastHeartbeats();

        // Node3 adopts leader's term 1 without ever forcing leader to step down
        assertEquals(NodeRole.LEADER, engine1.role(), "Leader must remain stable");
        assertEquals(Term.of(1), engine1.currentTerm(), "Cluster term must remain stable");
        assertEquals(Term.of(1), engine3.currentTerm(), "Rejoined node must adopt leader term");
    }

    @Test
    @DisplayName("Pre-candidate with quorum support transitions to candidate and wins election")
    void preCandidateWithQuorumWinsElection() {
        // Stop leader node1
        network.isolate(node1);

        // Node2 times out
        simulatedTime.addAndGet(500);
        engine2.checkElectionTimeout();

        // Node2 collected pre-vote from node3 (quorum 2/3), so it transitioned to CANDIDATE
        // and won full election in Term 2!
        assertEquals(NodeRole.LEADER, engine2.role());
        assertEquals(Term.of(2), engine2.currentTerm());
    }
}
