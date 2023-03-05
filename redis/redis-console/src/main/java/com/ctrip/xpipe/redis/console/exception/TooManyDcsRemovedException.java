package com.ctrip.xpipe.redis.console.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

public class TooManyDcsRemovedException extends XpipeRuntimeException {
    private static final long serialVersionUID = 1L;

    public TooManyDcsRemovedException(String message) {
        super(message);
    }

    public TooManyDcsRemovedException(String msg, Throwable th) {
        super(msg, th);
    }
}
