package com.engine.shardlock.domain.port;

import com.engine.shardlock.domain.model.NodeId;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Port for asynchronous peer-to-peer transport of Raft consensus RPCs.
 */
public interface TransportPort extends Closeable {

    interface RaftRpcHandler {
        RequestVoteResult handleRequestVote(RequestVoteArgs args);
        AppendEntriesResult handleAppendEntries(AppendEntriesArgs args);
    }

    void registerRpcHandler(RaftRpcHandler handler);

    CompletableFuture<RequestVoteResult> sendRequestVote(NodeId destination, RequestVoteArgs args);

    CompletableFuture<AppendEntriesResult> sendAppendEntries(NodeId destination, AppendEntriesArgs args);

    @Override
    void close();
}
