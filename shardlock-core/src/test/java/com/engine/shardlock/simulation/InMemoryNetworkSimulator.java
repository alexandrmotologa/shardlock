package com.engine.shardlock.simulation;

import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.port.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deterministic in-memory network simulator allowing controlled message delivery,
 * latency simulation, and network partition injection.
 */
public class InMemoryNetworkSimulator {

    private final Map<NodeId, TransportPort.RaftRpcHandler> handlers = new ConcurrentHashMap<>();
    private final Set<NodeId> isolatedNodes = ConcurrentHashMap.newKeySet();
    private final Set<String> partitionedLinks = ConcurrentHashMap.newKeySet();

    public TransportPort createTransportFor(NodeId nodeId) {
        return new SimulatedTransportPort(nodeId);
    }

    public void isolate(NodeId nodeId) {
        isolatedNodes.add(nodeId);
    }

    public void heal(NodeId nodeId) {
        isolatedNodes.remove(nodeId);
    }

    public void partition(NodeId from, NodeId to) {
        partitionedLinks.add(linkKey(from, to));
    }

    public void healPartition(NodeId from, NodeId to) {
        partitionedLinks.remove(linkKey(from, to));
    }

    public void clearAllPartitions() {
        isolatedNodes.clear();
        partitionedLinks.clear();
    }

    private String linkKey(NodeId a, NodeId b) {
        return a.value() + "->" + b.value();
    }

    private class SimulatedTransportPort implements TransportPort {
        private final NodeId localNodeId;

        public SimulatedTransportPort(NodeId localNodeId) {
            this.localNodeId = localNodeId;
        }

        @Override
        public void registerRpcHandler(RaftRpcHandler handler) {
            handlers.put(localNodeId, handler);
        }

        @Override
        public CompletableFuture<RequestVoteResult> sendRequestVote(NodeId destination, RequestVoteArgs args) {
            if (isBlocked(localNodeId, destination)) {
                return CompletableFuture.failedFuture(new RuntimeException("Network link partitioned"));
            }

            RaftRpcHandler target = handlers.get(destination);
            if (target == null) {
                return CompletableFuture.failedFuture(new RuntimeException("Node unreachable: " + destination));
            }

            try {
                RequestVoteResult res = target.handleRequestVote(args);
                return CompletableFuture.completedFuture(res);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public CompletableFuture<RequestVoteResult> sendPreVote(NodeId destination, RequestVoteArgs args) {
            if (isBlocked(localNodeId, destination)) {
                return CompletableFuture.failedFuture(new RuntimeException("Network link partitioned"));
            }

            RaftRpcHandler target = handlers.get(destination);
            if (target == null) {
                return CompletableFuture.failedFuture(new RuntimeException("Node unreachable: " + destination));
            }

            try {
                RequestVoteResult res = target.handlePreVote(args);
                return CompletableFuture.completedFuture(res);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public CompletableFuture<AppendEntriesResult> sendAppendEntries(NodeId destination, AppendEntriesArgs args) {
            if (isBlocked(localNodeId, destination)) {
                return CompletableFuture.failedFuture(new RuntimeException("Network link partitioned"));
            }

            RaftRpcHandler target = handlers.get(destination);
            if (target == null) {
                return CompletableFuture.failedFuture(new RuntimeException("Node unreachable: " + destination));
            }

            try {
                AppendEntriesResult res = target.handleAppendEntries(args);
                return CompletableFuture.completedFuture(res);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        private boolean isBlocked(NodeId from, NodeId to) {
            return isolatedNodes.contains(from)
                    || isolatedNodes.contains(to)
                    || partitionedLinks.contains(linkKey(from, to))
                    || partitionedLinks.contains(linkKey(to, from));
        }

        @Override
        public void close() {
            handlers.remove(localNodeId);
        }
    }
}
