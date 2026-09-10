package com.engine.shardlock.domain.port;

import com.engine.shardlock.domain.model.NodeId;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Port for asynchronous peer-to-peer transport of Raft consensus RPCs,
 * including standard elections and speculative Pre-Vote protocol exchanges.
 */
public interface TransportPort extends Closeable {

    interface RaftRpcHandler {
        RequestVoteResult handleRequestVote(RequestVoteArgs args);
        AppendEntriesResult handleAppendEntries(AppendEntriesArgs args);
        default RequestVoteResult handlePreVote(RequestVoteArgs args) {
            return handleRequestVote(args);
        }
    }

    void registerRpcHandler(RaftRpcHandler handler);

    CompletableFuture<RequestVoteResult> sendRequestVote(NodeId destination, RequestVoteArgs args);

    default CompletableFuture<RequestVoteResult> sendPreVote(NodeId destination, RequestVoteArgs args) {
        return sendRequestVote(destination, args);
    }

    CompletableFuture<AppendEntriesResult> sendAppendEntries(NodeId destination, AppendEntriesArgs args);

    @Override
    void close();
}
