package com.ctrip.xpipe.redis.console.service.logicalbu;

/** Carries an already-classified {@link OpFailure} up to the step that records it. */
public class KeeperOpException extends RuntimeException {

    private final OpFailure failure;

    public KeeperOpException(OpFailure failure, Throwable cause) {
        super(failure.toString(), cause);
        this.failure = failure;
    }

    public OpFailure getFailure() {
        return failure;
    }
}
