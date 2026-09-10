package com.engine.shardlock.domain.model;

import java.util.Objects;

/**
 * Immutable identifier for a node in the Raft cluster.
 */
public record NodeId(String value) implements Comparable<NodeId> {

    public NodeId {
        Objects.requireNonNull(value, "NodeId value cannot be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("NodeId value cannot be blank");
        }
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public int compareTo(NodeId o) {
        return this.value.compareTo(o.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
