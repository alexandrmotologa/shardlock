package com.engine.shardlock.infrastructure.server;

import com.engine.shardlock.application.service.LockManagerService;
import com.engine.shardlock.application.service.RaftConsensusEngine;
import com.engine.shardlock.domain.event.RaftDomainEvent;
import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.port.ClockPort;
import com.engine.shardlock.infrastructure.server.http.EmbeddedHttpServer;
import com.engine.shardlock.infrastructure.storage.wal.FileChannelWalStorage;
import com.engine.shardlock.infrastructure.transport.nio.AsyncTcpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Top-level runnable server node assembling the consensus engine, WAL storage,
 * async TCP P2P transport, lock manager, embedded HTTP server, and virtual thread schedulers.
 */
public class ShardLockServerNode implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ShardLockServerNode.class);

    private final NodeId nodeId;
    private final int httpPort;
    private final int peerPort;
    private final Path dataDir;

    private final FileChannelWalStorage storage;
    private final AsyncTcpTransport transport;
    private final RaftConsensusEngine consensusEngine;
    private final LockManagerService lockManager;
    private final EmbeddedHttpServer httpServer;

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ShardLockServerNode(
            NodeId nodeId,
            int httpPort,
            int peerPort,
            Map<NodeId, InetSocketAddress> peerAddresses,
            Map<NodeId, String> peerHttpUrls,
            Path dataDir
    ) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.httpPort = httpPort;
        this.peerPort = peerPort;
        this.dataDir = Objects.requireNonNull(dataDir);

        this.storage = new FileChannelWalStorage(dataDir);
        this.transport = new AsyncTcpTransport(nodeId, peerPort, peerAddresses);

        Set<NodeId> peerIds = peerAddresses.keySet();

        // Wire event publisher to HTTP server for SSE broadcasting
        this.consensusEngine = new RaftConsensusEngine(
                nodeId,
                peerIds,
                storage,
                transport,
                ClockPort.system(),
                this::onDomainEvent
        );

        this.lockManager = new LockManagerService(consensusEngine, ClockPort.system());
        this.httpServer = new EmbeddedHttpServer(httpPort, consensusEngine, lockManager, peerHttpUrls);

        this.scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
    }

    private void onDomainEvent(RaftDomainEvent event) {
        if (httpServer != null) {
            httpServer.publishEvent(event);
        }
    }

    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            httpServer.start();

            // Background heartbeat timer: 50ms
            scheduler.scheduleAtFixedRate(
                    consensusEngine::broadcastHeartbeats,
                    50, 50, TimeUnit.MILLISECONDS
            );

            // Background election timeout checker: 50ms
            scheduler.scheduleAtFixedRate(
                    consensusEngine::checkElectionTimeout,
                    50, 50, TimeUnit.MILLISECONDS
            );

            // Background expired lock reaper: 500ms
            scheduler.scheduleAtFixedRate(
                    lockManager::reapExpiredLocks,
                    500, 500, TimeUnit.MILLISECONDS
            );

            log.info("[{}] ShardLock server node online. HTTP: {}, Peer TCP: {}", nodeId, httpPort, peerPort);
        }
    }

    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdownNow();
            httpServer.close();
            transport.close();
            storage.close();
            log.info("[{}] ShardLock server node stopped", nodeId);
        }
    }

    @Override
    public void close() {
        stop();
    }

    public RaftConsensusEngine consensusEngine() {
        return consensusEngine;
    }

    public LockManagerService lockManager() {
        return lockManager;
    }

    public NodeId nodeId() {
        return nodeId;
    }

    public int httpPort() {
        return httpPort;
    }

    public int peerPort() {
        return peerPort;
    }
}
