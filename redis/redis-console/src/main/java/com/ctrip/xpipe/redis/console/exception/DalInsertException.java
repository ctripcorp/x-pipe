package com.ctrip.xpipe.redis.console.exception;

/**
 * @author chen.zhu
 * <p>
 * Mar 05, 2018
 */
public class DalInsertException extends RedisConsoleRuntimeException {
    private static final long serialVersionUID = 1L;

    public DalInsertException(String message) {
        super(message);
    }

    public DalInsertException(String msg, Throwable th) {
        super(msg, th);
    }
}
