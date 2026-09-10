package com.engine.shardlock.domain.event;

import com.engine.shardlock.domain.model.*;
import com.engine.shardlock.domain.state.LockRecord;

/**
 * Sealed hierarchy of domain events published during consensus lifecycle changes.
 */
public sealed interface RaftDomainEvent permits
        RaftDomainEvent.RoleChangedEvent,
        RaftDomainEvent.LeaderElectedEvent,
        RaftDomainEvent.TermIncrementedEvent,
        RaftDomainEvent.VoteGrantedEvent,
        RaftDomainEvent.LogAppendedEvent,
        RaftDomainEvent.LogCommittedEvent,
        RaftDomainEvent.LockAcquiredEvent,
        RaftDomainEvent.LockReleasedEvent,
        RaftDomainEvent.LockExpiredEvent {

    record RoleChangedEvent(NodeId nodeId, NodeRole oldRole, NodeRole newRole, Term term) implements RaftDomainEvent {}

    record LeaderElectedEvent(NodeId leaderId, Term term) implements RaftDomainEvent {}

    record TermIncrementedEvent(NodeId nodeId, Term oldTerm, Term newTerm) implements RaftDomainEvent {}

    record VoteGrantedEvent(NodeId voterId, NodeId candidateId, Term term) implements RaftDomainEvent {}

    record LogAppendedEvent(NodeId nodeId, LogEntry entry) implements RaftDomainEvent {}

    record LogCommittedEvent(NodeId nodeId, LogIndex commitIndex, LogEntry entry) implements RaftDomainEvent {}

    record LockAcquiredEvent(NodeId nodeId, LockRecord lockRecord) implements RaftDomainEvent {}

    record LockReleasedEvent(NodeId nodeId, String resource, FencingToken fencingToken) implements RaftDomainEvent {}

    record LockExpiredEvent(NodeId nodeId, String resource, FencingToken fencingToken) implements RaftDomainEvent {}
}
