package com.ctrip.xpipe.redis.meta.server.exception;

public class KeeperStateInCorrectException extends MetaServerRuntimeException {
    public KeeperStateInCorrectException(String message) {
        super(message);
    }

    public KeeperStateInCorrectException(String msg, Throwable th) {
        super(msg, th);
    }
}
