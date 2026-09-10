package com.engine.shardlock.client.cli;

import com.engine.shardlock.client.LockHandle;
import com.engine.shardlock.client.ShardLockClient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

/**
 * Standalone terminal CLI utility for inspecting, acquiring, renewing, and releasing
 * distributed leases across a ShardLock cluster.
 */
public class ShardLockCli {

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String endpoint = System.getenv().getOrDefault("SHARDLOCK_URL", "http://localhost:8001");
        List<String> remainingArgs = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            if ("--endpoint".equalsIgnoreCase(args[i]) || "-e".equalsIgnoreCase(args[i])) {
                if (i + 1 < args.length) {
                    endpoint = args[++i];
                }
            } else {
                remainingArgs.add(args[i]);
            }
        }

        if (remainingArgs.isEmpty()) {
            printUsage();
            System.exit(1);
        }

        String command = remainingArgs.get(0).toLowerCase();

        try (ShardLockClient client = ShardLockClient.builder()
                .endpoint(endpoint)
                .clientId("cli-" + System.getProperty("user.name", "user"))
                .build()) {

            switch (command) {
                case "status" -> handleStatus(client);
                case "metrics" -> handleMetrics(client);
                case "list", "locks" -> handleList(client);
                case "acquire" -> handleAcquire(client, remainingArgs);
                case "renew" -> handleRenew(client, remainingArgs);
                case "release" -> handleRelease(client, remainingArgs);
                case "watch" -> handleWatch(endpoint);
                default -> {
                    System.err.println("Unknown command: " + command);
                    printUsage();
                    System.exit(1);
                }
            }
        } catch (Exception e) {
            System.err.println("Execution error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void handleStatus(ShardLockClient client) throws Exception {
        Map<String, Object> status = client.getClusterStatus();
        System.out.println("=== ShardLock Cluster Status ===");
        System.out.println("Node ID:          " + status.get("nodeId"));
        System.out.println("Role:             " + status.get("role"));
        System.out.println("Current Term:     " + status.get("currentTerm"));
        System.out.println("Leader ID:        " + status.getOrDefault("leaderId", "None (Electing)"));
        System.out.println("Commit Index:     " + status.get("commitIndex"));
        System.out.println("Last Applied:     " + status.get("lastApplied"));
        System.out.println("Fencing Counter:  #" + status.get("fencingCounter"));
        System.out.println("Active Leases:    " + status.get("activeLockCount"));
        System.out.println("Wait Queue Size:  " + status.getOrDefault("waitQueueCount", 0));
        System.out.println("Peers:            " + status.get("peers"));
    }

    private static void handleMetrics(ShardLockClient client) throws Exception {
        String metrics = client.getMetrics();
        System.out.println(metrics);
    }

    private static void handleList(ShardLockClient client) throws Exception {
        List<Map<String, Object>> locks = client.listLocks();
        System.out.println("=== Active Partition Leases (" + locks.size() + ") ===");
        if (locks.isEmpty()) {
            System.out.println("No active leases held.");
            return;
        }

        System.out.printf("%-20s %-18s %-10s %-15s %-12s\n", "RESOURCE", "OWNER", "MODE", "FENCING TOKEN", "TTL REMAINING");
        System.out.println("--------------------------------------------------------------------------------");
        for (Map<String, Object> l : locks) {
            long remMs = ((Number) l.getOrDefault("remainingTtlMs", 0)).longValue();
            System.out.printf("%-20s %-18s %-10s #%-14s %-12s\n",
                    l.get("resource"),
                    l.get("ownerClientId"),
                    l.getOrDefault("mode", "EXCLUSIVE"),
                    l.get("fencingToken"),
                    String.format("%.1fs", remMs / 1000.0)
            );
        }
    }

    private static void handleAcquire(ShardLockClient client, List<String> args) {
        if (args.size() < 2) {
            System.err.println("Usage: shardlock-cli acquire <resource> [ttlMs] [waitTimeoutMs] [mode:EXCLUSIVE|SHARED]");
            System.exit(1);
        }

        String resource = args.get(1);
        long ttlMs = (args.size() > 2) ? Long.parseLong(args.get(2)) : 10000;
        long waitTimeoutMs = (args.size() > 3) ? Long.parseLong(args.get(3)) : 0;
        String mode = (args.size() > 4) ? args.get(4).toUpperCase() : "EXCLUSIVE";

        System.out.printf("Requesting %s lease for '%s' (ttl=%dms, wait=%dms)...\n", mode, resource, ttlMs, waitTimeoutMs);
        Optional<LockHandle> handle = client.acquire(resource, Duration.ofMillis(ttlMs), Duration.ofMillis(waitTimeoutMs), mode);

        if (handle.isPresent()) {
            LockHandle h = handle.get();
            System.out.println("Successfully acquired lease!");
            System.out.println("Resource:       " + h.resource());
            System.out.println("Client ID:      " + h.clientId());
            System.out.println("Mode:           " + h.mode());
            System.out.println("Fencing Token:  #" + h.fencingToken());
            System.out.println("Expires At:     " + new Date(h.expiresAtMs()));
        } else {
            System.err.println("Acquisition rejected. Resource occupied or wait timeout exceeded.");
            System.exit(1);
        }
    }

    private static void handleRenew(ShardLockClient client, List<String> args) {
        if (args.size() < 4) {
            System.err.println("Usage: shardlock-cli renew <resource> <clientId> <fencingToken> [ttlMs]");
            System.exit(1);
        }

        String resource = args.get(1);
        String clientId = args.get(2);
        long token = Long.parseLong(args.get(3));
        long ttlMs = (args.size() > 4) ? Long.parseLong(args.get(4)) : 10000;

        LockHandle handle = new LockHandle(resource, clientId, token, 0, Duration.ofMillis(ttlMs));
        Optional<LockHandle> renewed = client.renew(handle, Duration.ofMillis(ttlMs));

        if (renewed.isPresent()) {
            System.out.println("Successfully renewed lease until " + new Date(renewed.get().expiresAtMs()));
        } else {
            System.err.println("Renewal rejected. Token mismatch or lease already expired.");
            System.exit(1);
        }
    }

    private static void handleRelease(ShardLockClient client, List<String> args) {
        if (args.size() < 4) {
            System.err.println("Usage: shardlock-cli release <resource> <clientId> <fencingToken>");
            System.exit(1);
        }

        String resource = args.get(1);
        String clientId = args.get(2);
        long token = Long.parseLong(args.get(3));

        LockHandle handle = new LockHandle(resource, clientId, token, 0, Duration.ZERO);
        boolean success = client.release(handle);

        if (success) {
            System.out.println("Successfully released lease on '" + resource + "'");
        } else {
            System.err.println("Release rejected. Not lock owner or token mismatch.");
            System.exit(1);
        }
    }

    private static void handleWatch(String endpoint) throws Exception {
        String cleanEndpoint = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        String sseUrl = cleanEndpoint + "/api/v1/cluster/events";
        System.out.println("Connecting to event stream: " + sseUrl);

        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(sseUrl)).GET().build();

        http.send(req, HttpResponse.BodyHandlers.ofLines()).body().forEach(line -> {
            if (line.startsWith("data: ")) {
                System.out.println(line.substring(6));
            }
        });
    }

    private static void printUsage() {
        System.out.println("""
=== ShardLock CLI Usage ===
Usage: shardlock-cli [--endpoint <url>] <command> [args...]

Commands:
  status                                          Display cluster consensus & leader state
  metrics                                         Print Prometheus /metrics text output
  list                                            List all active partition leases
  acquire <resource> [ttlMs] [waitMs] [mode]      Acquire exclusive or shared lease
  renew <resource> <clientId> <token> [ttlMs]     Renew lease expiry before timeout
  release <resource> <clientId> <token>           Release actively held lease
  watch                                           Stream real-time SSE consensus events

Environment:
  SHARDLOCK_URL                                   Default cluster endpoint (default: http://localhost:8001)
""");
    }
}
