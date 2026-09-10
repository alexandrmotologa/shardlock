package com.engine.shardlock.domain.state;

import com.engine.shardlock.domain.model.FencingToken;

import java.util.Optional;

/**
 * Result of applying a command to the Raft state machine.
 */
public record StateMachineResult(
        Status status,
        String message,
        FencingToken fencingToken,
        LockRecord lockRecord
) {
    public enum Status {
        SUCCESS,
        REJECTED_ALREADY_HELD,
        REJECTED_TOKEN_MISMATCH,
        REJECTED_NOT_OWNER,
        REJECTED_NOT_FOUND,
        NOOP
    }

    public static StateMachineResult success(LockRecord record) {
        return new StateMachineResult(Status.SUCCESS, "Lock granted or renewed", record.fencingToken(), record);
    }

    public static StateMachineResult released(FencingToken token) {
        return new StateMachineResult(Status.SUCCESS, "Lock released", token, null);
    }

    public static StateMachineResult expired(String resource, FencingToken token) {
        return new StateMachineResult(Status.SUCCESS, "Lock expired: " + resource, token, null);
    }

    public static StateMachineResult noop() {
        return new StateMachineResult(Status.NOOP, "No-op applied", FencingToken.ZERO, null);
    }

    public static StateMachineResult rejectedAlreadyHeld(LockRecord existing) {
        return new StateMachineResult(
                Status.REJECTED_ALREADY_HELD,
                "Resource already held by " + existing.ownerClientId(),
                existing.fencingToken(),
                existing
        );
    }

    public static StateMachineResult rejectedTokenMismatch(long expected, long actual) {
        return new StateMachineResult(
                Status.REJECTED_TOKEN_MISMATCH,
                "Fencing token mismatch: expected " + expected + ", received " + actual,
                FencingToken.of(actual),
                null
        );
    }

    public static StateMachineResult rejectedNotOwner(String expectedOwner, String actualOwner) {
        return new StateMachineResult(
                Status.REJECTED_NOT_OWNER,
                "Client " + actualOwner + " does not own resource held by " + expectedOwner,
                FencingToken.ZERO,
                null
        );
    }

    public static StateMachineResult rejectedNotFound(String resource) {
        return new StateMachineResult(
                Status.REJECTED_NOT_FOUND,
                "No active lock found for resource: " + resource,
                FencingToken.ZERO,
                null
        );
    }

    public boolean isSuccess() {
        return status == Status.SUCCESS || status == Status.NOOP;
    }

    public Optional<LockRecord> getLockRecord() {
        return Optional.ofNullable(lockRecord);
    }
}
