package com.ctrip.xpipe.redis.console.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

public class LoadConsoleMetaException extends XpipeRuntimeException {
    private static final long serialVersionUID = 1L;

    public LoadConsoleMetaException(String message) {
        super(message);
    }

    public LoadConsoleMetaException(String msg, Throwable th) {
        super(msg, th);
    }
}
