package com.engine.shardlock.infrastructure.cli;

import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.infrastructure.server.ShardLockServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Command-line bootstrap entry point for starting a ShardLock daemon node.
 */
public class ShardLockServerCli {

    public static final String VERSION = "1.0.0";
    private static final Logger log = LoggerFactory.getLogger(ShardLockServerCli.class);

    public static void main(String[] args) {
        if (args != null && args.length > 0) {
            for (String arg : args) {
                if ("--version".equalsIgnoreCase(arg) || "-v".equalsIgnoreCase(arg) || "version".equalsIgnoreCase(arg)) {
                    System.out.println("shardlock-core version " + VERSION);
                    return;
                }
                if ("--help".equalsIgnoreCase(arg) || "-h".equalsIgnoreCase(arg) || "help".equalsIgnoreCase(arg)) {
                    printUsage();
                    return;
                }
            }
        }

        Map<String, String> params = parseArgs(args);

        String nodeIdStr = params.getOrDefault("node-id", "node-1");
        int httpPort = Integer.parseInt(params.getOrDefault("http-port", "8001"));
        int peerPort = Integer.parseInt(params.getOrDefault("peer-port", "9001"));
        String dataDirStr = params.getOrDefault("data-dir", "./data/" + nodeIdStr);
        String peersStr = params.getOrDefault("peers", "");

        NodeId nodeId = NodeId.of(nodeIdStr);
        Path dataDir = Paths.get(dataDirStr);

        Map<NodeId, InetSocketAddress> peerAddresses = new HashMap<>();
        Map<NodeId, String> peerHttpUrls = new HashMap<>();

        if (!peersStr.isBlank()) {
            for (String part : peersStr.split(",")) {
                String[] segments = part.trim().split(":");
                if (segments.length >= 3) {
                    NodeId peerId = NodeId.of(segments[0]);
                    if (!peerId.equals(nodeId)) {
                        String host = segments[1];
                        int port = Integer.parseInt(segments[2]);
                        peerAddresses.put(peerId, new InetSocketAddress(host, port));
                    }
                }
            }
        }

        log.info("Starting ShardLock daemon [{}]...", nodeId);
        log.info("HTTP Port: {}, Peer Port: {}, Data Directory: {}", httpPort, peerPort, dataDir.toAbsolutePath());
        log.info("Configured Peers: {}", peerAddresses.keySet());

        ShardLockServerNode node = new ShardLockServerNode(
                nodeId,
                httpPort,
                peerPort,
                peerAddresses,
                peerHttpUrls,
                dataDir
        );

        node.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered. Stopping ShardLock node [{}]...", nodeId);
            node.stop();
        }));

        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            log.info("Main thread interrupted. Halting.");
            node.stop();
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                int eq = arg.indexOf('=');
                if (eq > 2) {
                    String key = arg.substring(2, eq).trim();
                    String val = arg.substring(eq + 1).trim();
                    map.put(key, val);
                }
            }
        }
        return map;
    }

    private static void printUsage() {
        System.out.println("ShardLock Server Daemon - Distributed Consensus & Partition Lease Daemon");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -jar shardlock-core.jar [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --node-id=<id>           Cluster node identifier (default: node-1)");
        System.out.println("  --http-port=<port>       HTTP API port (default: 8001)");
        System.out.println("  --peer-port=<port>       Peer TCP port (default: 9001)");
        System.out.println("  --data-dir=<path>        Data directory for WAL storage");
        System.out.println("  --peers=<list>           Comma-separated peer endpoints (e.g. node-2:localhost:9002)");
        System.out.println("  -v, --version, version   Print version information and exit");
        System.out.println("  -h, --help, help         Print this help message and exit");
    }
}
