package com.engine.shardlock.application.service;

import com.engine.shardlock.domain.event.RaftDomainEvent;
import com.engine.shardlock.domain.model.*;
import com.engine.shardlock.domain.port.*;
import com.engine.shardlock.domain.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Raft consensus engine orchestrating leader election, log replication,
 * quorum calculation, and state machine commits.
 */
public class RaftConsensusEngine implements TransportPort.RaftRpcHandler {

    private static final Logger log = LoggerFactory.getLogger(RaftConsensusEngine.class);

    private final NodeId nodeId;
    private final Set<NodeId> peers;
    private final StoragePort storage;
    private final TransportPort transport;
    private final ClockPort clock;
    private final Consumer<RaftDomainEvent> eventPublisher;

    private final RaftLog raftLog;
    private final RaftStateMachine stateMachine;

    private NodeRole role = NodeRole.FOLLOWER;
    private Term currentTerm = Term.ZERO;
    private NodeId votedFor = null;
    private NodeId currentLeaderId = null;

    private final Map<NodeId, LogIndex> nextIndex = new ConcurrentHashMap<>();
    private final Map<NodeId, LogIndex> matchIndex = new ConcurrentHashMap<>();
    private final Map<LogIndex, CompletableFuture<StateMachineResult>> pendingProposals = new ConcurrentHashMap<>();

    private long lastHeartbeatOrElectionResetMs;
    private long electionTimeoutMs;
    private final Random random = new Random();

    private final int snapshotThreshold;
    private long entriesAppliedSinceSnapshot = 0;

    public RaftConsensusEngine(
            NodeId nodeId,
            Set<NodeId> peers,
            StoragePort storage,
            TransportPort transport,
            ClockPort clock,
            Consumer<RaftDomainEvent> eventPublisher
    ) {
        this(nodeId, peers, storage, transport, clock, eventPublisher, 500);
    }

    public RaftConsensusEngine(
            NodeId nodeId,
            Set<NodeId> peers,
            StoragePort storage,
            TransportPort transport,
            ClockPort clock,
            Consumer<RaftDomainEvent> eventPublisher,
            int snapshotThreshold
    ) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.peers = Set.copyOf(peers);
        this.storage = Objects.requireNonNull(storage);
        this.transport = Objects.requireNonNull(transport);
        this.clock = Objects.requireNonNull(clock);
        this.eventPublisher = (eventPublisher == null) ? (e -> {}) : eventPublisher;
        this.snapshotThreshold = snapshotThreshold;

        this.raftLog = new RaftLog();
        this.stateMachine = new RaftStateMachine();

        // Restore persisted state from storage
        loadFromStorage();
        resetElectionTimeout();
        this.transport.registerRpcHandler(this);
    }

    private void loadFromStorage() {
        // Load snapshot if available
        Optional<StateMachineSnapshot> snapshotOpt = storage.readSnapshot();
        if (snapshotOpt.isPresent()) {
            StateMachineSnapshot snapshot = snapshotOpt.get();
            stateMachine.restore(snapshot);
            raftLog.compact(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
        }

        // Load metadata
        StoragePort.StorageMetadata metadata = storage.readMetadata();
        if (metadata != null) {
            this.currentTerm = metadata.currentTerm();
            this.votedFor = metadata.votedFor();
        }

        // Load log entries
        List<LogEntry> entries = storage.readAllEntries();
        for (LogEntry entry : entries) {
            raftLog.appendRaw(entry);
        }
    }

    public synchronized void resetElectionTimeout() {
        this.lastHeartbeatOrElectionResetMs = clock.currentTimeMillis();
        // Randomized election timeout: 150ms to 300ms
        this.electionTimeoutMs = 150 + random.nextInt(151);
    }

    public synchronized boolean hasElectionTimeoutElapsed() {
        return (clock.currentTimeMillis() - lastHeartbeatOrElectionResetMs) >= electionTimeoutMs;
    }

    public synchronized void checkElectionTimeout() {
        if (role != NodeRole.LEADER && hasElectionTimeoutElapsed()) {
            log.info("[{}] Election timeout elapsed ({} ms). Starting pre-vote check.", nodeId, electionTimeoutMs);
            startPreVote();
        }
    }

    public synchronized void startPreVote() {
        if (peers.isEmpty()) {
            startElection();
            return;
        }

        transitionTo(NodeRole.PRE_CANDIDATE);
        resetElectionTimeout();
        Term nextTerm = currentTerm.next();

        int quorum = (peers.size() + 1) / 2 + 1;
        AtomicInteger preVotesReceived = new AtomicInteger(1); // Self vote

        RequestVoteArgs args = new RequestVoteArgs(
                nextTerm,
                nodeId,
                raftLog.lastIndex(),
                raftLog.lastTerm()
        );

        for (NodeId peer : peers) {
            transport.sendPreVote(peer, args).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.debug("[{}] Error requesting pre-vote from {}: {}", nodeId, peer, ex.getMessage());
                    return;
                }
                handlePreVoteResponse(peer, result, nextTerm, quorum, preVotesReceived);
            });
        }
    }

    private synchronized void handlePreVoteResponse(
            NodeId peer,
            RequestVoteResult result,
            Term candidateTerm,
            int quorum,
            AtomicInteger preVotesReceived
    ) {
        if (result.term().isGreaterThan(currentTerm)) {
            stepDown(result.term());
            return;
        }

        if (role == NodeRole.PRE_CANDIDATE && currentTerm.next().equals(candidateTerm) && result.voteGranted()) {
            int currentVotes = preVotesReceived.incrementAndGet();
            log.debug("[{}] Received pre-vote from {} (total: {}/{})", nodeId, peer, currentVotes, quorum);
            if (currentVotes >= quorum) {
                log.info("[{}] Pre-vote quorum granted ({}/{}). Starting full election.", nodeId, currentVotes, quorum);
                startElection();
            }
        }
    }

    public synchronized void startElection() {
        transitionTo(NodeRole.CANDIDATE);
        this.currentTerm = currentTerm.next();
        this.votedFor = nodeId;
        this.currentLeaderId = null;
        persistMetadata();
        resetElectionTimeout();

        eventPublisher.accept(new RaftDomainEvent.TermIncrementedEvent(nodeId, Term.of(currentTerm.value() - 1), currentTerm));

        log.info("[{}] Starting election for term {}", nodeId, currentTerm);

        // A single-node cluster immediately wins
        if (peers.isEmpty()) {
            becomeLeader();
            return;
        }

        int quorum = (peers.size() + 1) / 2 + 1;
        AtomicInteger votesReceived = new AtomicInteger(1); // Vote for self

        RequestVoteArgs args = new RequestVoteArgs(
                currentTerm,
                nodeId,
                raftLog.lastIndex(),
                raftLog.lastTerm()
        );

        Term electionTerm = currentTerm;

        for (NodeId peer : peers) {
            transport.sendRequestVote(peer, args).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.debug("[{}] Error requesting vote from {}: {}", nodeId, peer, ex.getMessage());
                    return;
                }
                handleVoteResponse(peer, result, electionTerm, quorum, votesReceived);
            });
        }
    }

    private synchronized void handleVoteResponse(
            NodeId peer,
            RequestVoteResult result,
            Term electionTerm,
            int quorum,
            AtomicInteger votesReceived
    ) {
        if (result.term().isGreaterThan(currentTerm)) {
            stepDown(result.term());
            return;
        }

        if (role == NodeRole.CANDIDATE && currentTerm.equals(electionTerm) && result.voteGranted()) {
            int currentVotes = votesReceived.incrementAndGet();
            log.debug("[{}] Received vote from {} (total: {}/{})", nodeId, peer, currentVotes, quorum);
            if (currentVotes >= quorum) {
                becomeLeader();
            }
        }
    }

    private synchronized void becomeLeader() {
        if (role == NodeRole.LEADER) {
            return;
        }
        transitionTo(NodeRole.LEADER);
        this.currentLeaderId = nodeId;
        log.info("[{}] Elected as LEADER for term {}", nodeId, currentTerm);

        // Initialize leader tracking state
        LogIndex nextLogIndex = raftLog.lastIndex().next();
        for (NodeId peer : peers) {
            nextIndex.put(peer, nextLogIndex);
            matchIndex.put(peer, LogIndex.ZERO);
        }

        eventPublisher.accept(new RaftDomainEvent.LeaderElectedEvent(nodeId, currentTerm));

        // Propose initial noop command to establish commit authority in new term
        propose(LockCommand.noop(clock.currentTimeMillis()));

        // Broadcast immediate heartbeat
        broadcastHeartbeats();
    }

    public synchronized void stepDown(Term newTerm) {
        log.info("[{}] Stepping down to FOLLOWER in term {} (was in term {})", nodeId, newTerm, currentTerm);
        this.currentTerm = newTerm;
        this.votedFor = null;
        this.currentLeaderId = null;
        transitionTo(NodeRole.FOLLOWER);
        persistMetadata();
        resetElectionTimeout();
    }

    private void transitionTo(NodeRole newRole) {
        if (this.role != newRole) {
            NodeRole old = this.role;
            this.role = newRole;
            eventPublisher.accept(new RaftDomainEvent.RoleChangedEvent(nodeId, old, newRole, currentTerm));
        }
    }

    @Override
    public synchronized RequestVoteResult handleRequestVote(RequestVoteArgs args) {
        if (args.term().isGreaterThan(currentTerm)) {
            stepDown(args.term());
        }

        if (args.term().isLessThan(currentTerm)) {
            return new RequestVoteResult(currentTerm, false, nodeId);
        }

        boolean canVote = (votedFor == null || votedFor.equals(args.candidateId()));
        boolean logIsUpToDate = isCandidateLogUpToDate(args.lastLogTerm(), args.lastLogIndex());

        if (canVote && logIsUpToDate) {
            this.votedFor = args.candidateId();
            persistMetadata();
            resetElectionTimeout();
            eventPublisher.accept(new RaftDomainEvent.VoteGrantedEvent(nodeId, args.candidateId(), currentTerm));
            log.info("[{}] Granted vote to {} in term {}", nodeId, args.candidateId(), currentTerm);
            return new RequestVoteResult(currentTerm, true, nodeId);
        }

        return new RequestVoteResult(currentTerm, false, nodeId);
    }

    private boolean isCandidateLogUpToDate(Term candidateTerm, LogIndex candidateIndex) {
        Term ourTerm = raftLog.lastTerm();
        LogIndex ourIndex = raftLog.lastIndex();

        if (candidateTerm.isGreaterThan(ourTerm)) {
            return true;
        }
        if (candidateTerm.equals(ourTerm)) {
            return !candidateIndex.isLessThan(ourIndex);
        }
        return false;
    }

    @Override
    public synchronized RequestVoteResult handlePreVote(RequestVoteArgs args) {
        if (args.term().isLessThan(currentTerm)) {
            return new RequestVoteResult(currentTerm, false, nodeId);
        }

        // Under Pre-Vote, do NOT step down or increment term.
        // Just verify whether the candidate's log is up-to-date and whether leader lease is expired.
        boolean logIsUpToDate = isCandidateLogUpToDate(args.lastLogTerm(), args.lastLogIndex());
        boolean leaderLeaseExpired = hasElectionTimeoutElapsed() || currentLeaderId == null;

        if (logIsUpToDate && leaderLeaseExpired) {
            log.debug("[{}] Granted pre-vote to {} for prospective term {}", nodeId, args.candidateId(), args.term());
            return new RequestVoteResult(currentTerm, true, nodeId);
        }

        return new RequestVoteResult(currentTerm, false, nodeId);
    }

    @Override
    public synchronized AppendEntriesResult handleAppendEntries(AppendEntriesArgs args) {
        if (args.term().isGreaterThan(currentTerm)) {
            stepDown(args.term());
        }

        if (args.term().isLessThan(currentTerm)) {
            return new AppendEntriesResult(currentTerm, false, raftLog.lastIndex(), nodeId);
        }

        // Valid leader recognized
        if (role == NodeRole.CANDIDATE) {
            transitionTo(NodeRole.FOLLOWER);
        }
        this.currentLeaderId = args.leaderId();
        resetElectionTimeout();

        // Check log consistency at prevLogIndex
        if (args.prevLogIndex().value() > 0) {
            Optional<Term> termAtPrev = raftLog.getTerm(args.prevLogIndex());
            if (termAtPrev.isEmpty() || !termAtPrev.get().equals(args.prevLogTerm())) {
                log.debug("[{}] Log inconsistency at index {} (expected term: {}, have: {:?})",
                        nodeId, args.prevLogIndex(), args.prevLogTerm(), termAtPrev);
                return new AppendEntriesResult(currentTerm, false, raftLog.lastIndex(), nodeId);
            }
        }

        // Process new entries
        if (!args.entries().isEmpty()) {
            for (LogEntry entry : args.entries()) {
                Optional<LogEntry> existing = raftLog.getEntry(entry.index());
                if (existing.isPresent()) {
                    if (!existing.get().term().equals(entry.term())) {
                        // Conflict: truncate log and storage from this point
                        log.warn("[{}] Log conflict at index {}. Truncating suffix.", nodeId, entry.index());
                        raftLog.truncateSuffix(entry.index());
                        storage.truncateSuffix(entry.index());
                        raftLog.appendRaw(entry);
                        storage.appendEntries(List.of(entry));
                    }
                } else {
                    raftLog.appendRaw(entry);
                    storage.appendEntries(List.of(entry));
                }
            }
        }

        // Update commit index
        if (args.leaderCommit().isGreaterThan(raftLog.commitIndex())) {
            long newCommit = Math.min(args.leaderCommit().value(), raftLog.lastIndex().value());
            LogIndex newCommitIndex = LogIndex.of(newCommit);
            applyEntriesUpTo(newCommitIndex);
        }

        return new AppendEntriesResult(currentTerm, true, raftLog.lastIndex(), nodeId);
    }

    public synchronized CompletableFuture<StateMachineResult> propose(LockCommand command) {
        if (role != NodeRole.LEADER) {
            CompletableFuture<StateMachineResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(new NotLeaderException(currentLeaderId));
            return failed;
        }

        LogEntry entry = raftLog.append(currentTerm, command);
        storage.appendEntries(List.of(entry));
        eventPublisher.accept(new RaftDomainEvent.LogAppendedEvent(nodeId, entry));

        CompletableFuture<StateMachineResult> future = new CompletableFuture<>();
        pendingProposals.put(entry.index(), future);

        // Single node cluster commits immediately
        if (peers.isEmpty()) {
            applyEntriesUpTo(entry.index());
            return future;
        }

        // Trigger replication to peers
        replicateToPeers();
        return future;
    }

    public synchronized void broadcastHeartbeats() {
        if (role != NodeRole.LEADER) {
            return;
        }
        replicateToPeers();
    }

    private void replicateToPeers() {
        for (NodeId peer : peers) {
            sendAppendEntriesTo(peer);
        }
    }

    private void sendAppendEntriesTo(NodeId peer) {
        LogIndex peerNext = nextIndex.getOrDefault(peer, LogIndex.of(1));
        LogIndex prevIndex = peerNext.prev();
        Term prevTerm = raftLog.getTerm(prevIndex).orElse(Term.ZERO);

        List<LogEntry> entries = raftLog.getEntriesFrom(peerNext, 50);

        AppendEntriesArgs args = new AppendEntriesArgs(
                currentTerm,
                nodeId,
                prevIndex,
                prevTerm,
                entries,
                raftLog.commitIndex()
        );

        Term requestTerm = currentTerm;

        transport.sendAppendEntries(peer, args).whenComplete((result, ex) -> {
            if (ex != null) {
                log.debug("[{}] Error sending AppendEntries to {}: {}", nodeId, peer, ex.getMessage());
                return;
            }
            handleAppendEntriesResponse(peer, result, requestTerm, args);
        });
    }

    private synchronized void handleAppendEntriesResponse(
            NodeId peer,
            AppendEntriesResult result,
            Term requestTerm,
            AppendEntriesArgs sentArgs
    ) {
        if (result.term().isGreaterThan(currentTerm)) {
            stepDown(result.term());
            return;
        }

        if (role != NodeRole.LEADER || !currentTerm.equals(requestTerm)) {
            return;
        }

        if (result.success()) {
            LogIndex newMatch = sentArgs.entries().isEmpty()
                    ? sentArgs.prevLogIndex()
                    : sentArgs.entries().get(sentArgs.entries().size() - 1).index();

            matchIndex.put(peer, newMatch);
            nextIndex.put(peer, newMatch.next());

            checkAndUpdateCommitIndex();
        } else {
            // Decrement nextIndex and retry on next replication cycle
            LogIndex currentNext = nextIndex.getOrDefault(peer, LogIndex.of(1));
            if (currentNext.value() > 1) {
                nextIndex.put(peer, currentNext.prev());
            }
        }
    }

    private void checkAndUpdateCommitIndex() {
        int totalNodes = peers.size() + 1;
        int quorum = totalNodes / 2 + 1;

        LogIndex lastLog = raftLog.lastIndex();
        for (long i = lastLog.value(); i > raftLog.commitIndex().value(); i--) {
            LogIndex index = LogIndex.of(i);
            Optional<Term> term = raftLog.getTerm(index);

            // Raft safety invariant: Only commit entries from the leader's current term directly
            if (term.isPresent() && term.get().equals(currentTerm)) {
                int matches = 1; // Count leader itself
                for (NodeId peer : peers) {
                    if (!matchIndex.getOrDefault(peer, LogIndex.ZERO).isLessThan(index)) {
                        matches++;
                    }
                }
                if (matches >= quorum) {
                    applyEntriesUpTo(index);
                    break;
                }
            }
        }
    }

    private void applyEntriesUpTo(LogIndex newCommitIndex) {
        raftLog.setCommitIndex(newCommitIndex);

        while (raftLog.lastApplied().isLessThan(raftLog.commitIndex())) {
            LogIndex nextToApply = raftLog.lastApplied().next();
            Optional<LogEntry> entryOpt = raftLog.getEntry(nextToApply);
            if (entryOpt.isPresent()) {
                LogEntry entry = entryOpt.get();
                StateMachineResult result = stateMachine.apply(entry);
                raftLog.setLastApplied(nextToApply);

                eventPublisher.accept(new RaftDomainEvent.LogCommittedEvent(nodeId, nextToApply, entry));

                entriesAppliedSinceSnapshot++;
                if (snapshotThreshold > 0 && entriesAppliedSinceSnapshot >= snapshotThreshold) {
                    entriesAppliedSinceSnapshot = 0;
                    triggerSnapshot();
                }

                CompletableFuture<StateMachineResult> pending = pendingProposals.remove(nextToApply);
                if (pending != null) {
                    pending.complete(result);
                }
            } else {
                break;
            }
        }
    }

    public synchronized void triggerSnapshot() {
        LogIndex lastApplied = raftLog.lastApplied();
        if (lastApplied.value() <= 0) {
            return;
        }
        Term lastTerm = raftLog.getTerm(lastApplied).orElse(Term.ZERO);
        long now = clock.currentTimeMillis();
        StateMachineSnapshot snap = stateMachine.snapshot(lastApplied, lastTerm, now);

        Thread.ofVirtual().name("raft-snapshot-" + nodeId).start(() -> {
            try {
                storage.saveSnapshot(snap);
                synchronized (RaftConsensusEngine.this) {
                    raftLog.compact(lastApplied, lastTerm);
                }
                log.info("[{}] State machine snapshot taken at index {}, term {}", nodeId, lastApplied, lastTerm);
            } catch (Exception e) {
                log.error("[{}] Error persisting state machine snapshot: {}", nodeId, e.getMessage(), e);
            }
        });
    }

    public synchronized CompletableFuture<Void> readIndex() {
        if (role != NodeRole.LEADER) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new NotLeaderException(currentLeaderId));
            return failed;
        }

        if (peers.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        int quorum = (peers.size() + 1) / 2 + 1;
        AtomicInteger acks = new AtomicInteger(1);
        Term leaderTerm = currentTerm;

        for (NodeId peer : peers) {
            LogIndex peerNext = nextIndex.getOrDefault(peer, LogIndex.of(1));
            LogIndex prevIndex = peerNext.prev();
            Term prevTerm = raftLog.getTerm(prevIndex).orElse(Term.ZERO);

            AppendEntriesArgs heartbeat = new AppendEntriesArgs(
                    leaderTerm,
                    nodeId,
                    prevIndex,
                    prevTerm,
                    List.of(),
                    raftLog.commitIndex()
            );

            transport.sendAppendEntries(peer, heartbeat).whenComplete((res, ex) -> {
                if (ex == null && res.success() && res.term().equals(leaderTerm)) {
                    if (acks.incrementAndGet() >= quorum && !future.isDone()) {
                        future.complete(null);
                    }
                }
            });
        }

        return future;
    }

    private void persistMetadata() {
        storage.saveMetadata(currentTerm, votedFor);
    }

    // Accessors for observation and testing
    public NodeId nodeId() { return nodeId; }
    public Set<NodeId> peers() { return peers; }
    public synchronized NodeRole role() { return role; }
    public synchronized Term currentTerm() { return currentTerm; }
    public synchronized NodeId votedFor() { return votedFor; }
    public synchronized NodeId currentLeaderId() { return currentLeaderId; }
    public RaftLog raftLog() { return raftLog; }
    public RaftStateMachine stateMachine() { return stateMachine; }
    public synchronized Map<NodeId, LogIndex> matchIndex() { return Collections.unmodifiableMap(new HashMap<>(matchIndex)); }

    public static class NotLeaderException extends RuntimeException {
        private final NodeId leaderId;

        public NotLeaderException(NodeId leaderId) {
            super(leaderId != null ? "Not leader. Current leader is " + leaderId : "Not leader. Leader is unknown");
            this.leaderId = leaderId;
        }

        public NodeId leaderId() {
            return leaderId;
        }
    }
}
