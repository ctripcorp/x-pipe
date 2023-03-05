package com.ctrip.xpipe.redis.console.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

public class TooManyClustersRemovedException extends XpipeRuntimeException {
    private static final long serialVersionUID = 1L;

    public TooManyClustersRemovedException(String message) {
        super(message);
    }

    public TooManyClustersRemovedException(String msg, Throwable th) {
        super(msg, th);
    }
}
