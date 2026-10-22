package com.ctrip.xpipe.redis.core.exception;

/**
 * {@code PSYNC ? -4} (and any client that must not take RDB) rejects
 * FULLRESYNC / XFULLRESYNC / the RDB reader path. Callers match {@code instanceof},
 * not exception text.
 */
public class RdbRejectedException extends RedisRuntimeException {

    private static final long serialVersionUID = 1L;

    public RdbRejectedException(String method) {
        super(message(method));
    }

    public RdbRejectedException(String method, Throwable cause) {
        super(message(method), cause);
    }

    private static String message(String method) {
        return "CmdTailGapAllowedSync." + method + ": RDB not allowed";
    }
}
