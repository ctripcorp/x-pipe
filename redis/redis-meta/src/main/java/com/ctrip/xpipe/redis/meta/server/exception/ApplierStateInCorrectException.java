package com.ctrip.xpipe.redis.meta.server.exception;

/**
 * @author ayq
 * <p>
 * 2022/4/7 16:35
 */
public class ApplierStateInCorrectException extends MetaServerRuntimeException {
    public ApplierStateInCorrectException(String message) {
        super(message);
    }

    public ApplierStateInCorrectException(String msg, Throwable th) {
        super(msg, th);
    }
}
