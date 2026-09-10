package com.engine.shardlock.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * High-performance Java 21 client SDK for ShardLock with cluster auto-discovery,
 * transparent redirect following, background lease renewal, and fencing token propagation.
 */
public class ShardLockClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ShardLockClient.class);

    private final List<String> endpoints;
    private final String clientId;
    private final Duration requestTimeout;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();

    private final AtomicReference<String> knownLeaderEndpoint = new AtomicReference<>();

    private ShardLockClient(Builder builder) {
        this.endpoints = new ArrayList<>(builder.endpoints);
        this.clientId = builder.clientId;
        this.requestTimeout = builder.requestTimeout;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(builder.requestTimeout)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        if (!endpoints.isEmpty()) {
            this.knownLeaderEndpoint.set(endpoints.get(0));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<LockHandle> acquire(String resource, Duration ttl) {
        return acquire(resource, ttl, Duration.ZERO, "EXCLUSIVE");
    }

    public Optional<LockHandle> acquire(String resource, Duration ttl, Duration waitTimeout, String mode) {
        Map<String, Object> reqBody = new LinkedHashMap<>();
        reqBody.put("resource", resource);
        reqBody.put("clientId", clientId);
        reqBody.put("ttlMs", ttl.toMillis());
        reqBody.put("waitTimeoutMs", waitTimeout.toMillis());
        reqBody.put("mode", mode != null ? mode : "EXCLUSIVE");

        try {
            HttpResponse<String> resp = sendRequestWithFailover("/api/v1/locks/acquire", reqBody);
            if (resp.statusCode() == 200) {
                Map<String, Object> data = mapper.readValue(resp.body(), Map.class);
                long token = ((Number) data.get("fencingToken")).longValue();
                long expiresAtMs = ((Number) data.get("expiresAtMs")).longValue();
                String grantedMode = (String) data.getOrDefault("mode", mode != null ? mode : "EXCLUSIVE");
                LockHandle handle = new LockHandle(resource, clientId, token, expiresAtMs, ttl, grantedMode);
                return Optional.of(handle);
            } else if (resp.statusCode() == 409) {
                log.debug("Resource '{}' is currently held or wait queue expired", resource);
                return Optional.empty();
            } else {
                log.warn("Acquire lock failed with status {}: {}", resp.statusCode(), resp.body());
                return Optional.empty();
            }
        } catch (Exception e) {
            log.error("Failed to acquire lock for '{}': {}", resource, e.getMessage());
            return Optional.empty();
        }
    }

    public Optional<LockHandle> acquireShared(String resource, Duration ttl, Duration waitTimeout) {
        return acquire(resource, ttl, waitTimeout, "SHARED");
    }

    public boolean tryWithSharedLock(String resource, Duration ttl, Consumer<LockHandle> action) {
        Optional<LockHandle> handleOpt = acquireShared(resource, ttl, Duration.ZERO);
        if (handleOpt.isEmpty()) {
            return false;
        }
        try {
            action.accept(handleOpt.get());
            return true;
        } finally {
            release(handleOpt.get());
        }
    }

    public Optional<LockHandle> renew(LockHandle handle, Duration ttl) {
        Map<String, Object> reqBody = Map.of(
                "resource", handle.resource(),
                "clientId", handle.clientId(),
                "fencingToken", handle.fencingToken(),
                "ttlMs", ttl.toMillis()
        );

        try {
            HttpResponse<String> resp = sendRequestWithFailover("/api/v1/locks/renew", reqBody);
            if (resp.statusCode() == 200) {
                Map<String, Object> data = mapper.readValue(resp.body(), Map.class);
                long expiresAtMs = ((Number) data.get("expiresAtMs")).longValue();
                return Optional.of(new LockHandle(handle.resource(), handle.clientId(), handle.fencingToken(), expiresAtMs, ttl));
            }
            return Optional.empty();
        } catch (Exception e) {
            log.warn("Failed to renew lock '{}': {}", handle.resource(), e.getMessage());
            return Optional.empty();
        }
    }

    public boolean release(LockHandle handle) {
        Map<String, Object> reqBody = Map.of(
                "resource", handle.resource(),
                "clientId", handle.clientId(),
                "fencingToken", handle.fencingToken()
        );

        try {
            HttpResponse<String> resp = sendRequestWithFailover("/api/v1/locks/release", reqBody);
            return resp.statusCode() == 200;
        } catch (Exception e) {
            log.warn("Failed to release lock '{}': {}", handle.resource(), e.getMessage());
            return false;
        }
    }

    public boolean tryWithLock(String resource, Duration ttl, Consumer<LockHandle> action) {
        return tryWithLock(resource, ttl, handle -> {
            action.accept(handle);
            return true;
        }).orElse(false);
    }

    public <R> Optional<R> tryWithLock(String resource, Duration ttl, Function<LockHandle, R> action) {
        Optional<LockHandle> handleOpt = acquire(resource, ttl);
        if (handleOpt.isEmpty()) {
            return Optional.empty();
        }

        LockHandle initialHandle = handleOpt.get();
        AtomicReference<LockHandle> currentHandle = new AtomicReference<>(initialHandle);
        AtomicBoolean active = new AtomicBoolean(true);

        // Start virtual thread background heartbeat loop
        Thread heartbeatThread = Thread.ofVirtual().name("shardlock-heartbeat-" + resource).start(() -> {
            long renewalIntervalMs = Math.max(200, ttl.toMillis() / 3);
            while (active.get()) {
                try {
                    Thread.sleep(renewalIntervalMs);
                    if (!active.get()) break;

                    Optional<LockHandle> renewed = renew(currentHandle.get(), ttl);
                    if (renewed.isPresent()) {
                        currentHandle.set(renewed.get());
                    } else {
                        log.warn("Heartbeat renewal failed for resource: {}", resource);
                        break;
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    log.warn("Error during background lock heartbeat: {}", e.getMessage());
                }
            }
        });

        try {
            R result = action.apply(currentHandle.get());
            return Optional.ofNullable(result);
        } finally {
            active.set(false);
            heartbeatThread.interrupt();
            release(currentHandle.get());
        }
    }

    private HttpResponse<String> sendRequestWithFailover(String path, Object body) throws Exception {
        byte[] payload = mapper.writeValueAsBytes(body);
        List<String> targetUrls = buildCandidateUrls(path);

        Exception lastException = null;
        for (String url : targetUrls) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(requestTimeout)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                        .build();

                HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

                // If redirected to leader, update known leader
                if (resp.statusCode() == 307) {
                    Optional<String> location = resp.headers().firstValue("Location");
                    if (location.isPresent()) {
                        String leaderUrl = location.get();
                        updateKnownLeaderFromUrl(leaderUrl);

                        HttpRequest redirectReq = HttpRequest.newBuilder()
                                .uri(URI.create(leaderUrl))
                                .timeout(requestTimeout)
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                                .build();
                        return httpClient.send(redirectReq, HttpResponse.BodyHandlers.ofString());
                    }
                }

                if (resp.statusCode() != 503) {
                    updateKnownLeaderFromUrl(url);
                    return resp;
                }
            } catch (Exception e) {
                lastException = e;
                log.debug("Failed contacting endpoint {}: {}", url, e.getMessage());
            }
        }

        throw new IOException("Unable to reach active ShardLock cluster leader after trying endpoints: " + targetUrls, lastException);
    }

    private List<String> buildCandidateUrls(String path) {
        List<String> candidates = new ArrayList<>();
        String leader = knownLeaderEndpoint.get();
        if (leader != null) {
            candidates.add(cleanUrl(leader) + path);
        }
        for (String ep : endpoints) {
            String full = cleanUrl(ep) + path;
            if (!candidates.contains(full)) {
                candidates.add(full);
            }
        }
        return candidates;
    }

    private void updateKnownLeaderFromUrl(String fullUrl) {
        try {
            URI uri = URI.create(fullUrl);
            String base = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort();
            knownLeaderEndpoint.set(base);
        } catch (Exception ignored) {
        }
    }

    public String getMetrics() throws Exception {
        HttpResponse<String> resp = sendGetRequestWithFailover("/metrics");
        return resp.body();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getClusterStatus() throws Exception {
        HttpResponse<String> resp = sendGetRequestWithFailover("/api/v1/cluster/status");
        return mapper.readValue(resp.body(), Map.class);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> listLocks() throws Exception {
        HttpResponse<String> resp = sendGetRequestWithFailover("/api/v1/locks");
        return mapper.readValue(resp.body(), List.class);
    }

    private HttpResponse<String> sendGetRequestWithFailover(String path) throws Exception {
        List<String> targetUrls = buildCandidateUrls(path);
        Exception lastException = null;
        for (String url : targetUrls) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(requestTimeout)
                        .GET()
                        .build();

                HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    updateKnownLeaderFromUrl(url);
                    return resp;
                }
            } catch (Exception e) {
                lastException = e;
            }
        }
        throw new IOException("Unable to reach ShardLock endpoint: " + path, lastException);
    }

    private String cleanUrl(String ep) {
        return ep.endsWith("/") ? ep.substring(0, ep.length() - 1) : ep;
    }

    @Override
    public void close() {
    }

    public static class Builder {
        private List<String> endpoints = new ArrayList<>();
        private String clientId = "shardlock-client-" + UUID.randomUUID();
        private Duration requestTimeout = Duration.ofSeconds(5);

        public Builder endpoints(List<String> endpoints) {
            this.endpoints = new ArrayList<>(endpoints);
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoints.add(endpoint);
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public ShardLockClient build() {
            if (endpoints.isEmpty()) {
                endpoints.add("http://localhost:8001");
            }
            return new ShardLockClient(this);
        }
    }
}
