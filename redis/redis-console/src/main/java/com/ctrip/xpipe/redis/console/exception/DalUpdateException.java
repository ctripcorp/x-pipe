package com.ctrip.xpipe.redis.console.exception;

/**
 * @author chen.zhu
 * <p>
 * Mar 01, 2018
 */
public class DalUpdateException extends RedisConsoleRuntimeException {

    private static final long serialVersionUID = 1L;

    public DalUpdateException(String message) {
        super(message);
    }

    public DalUpdateException(String msg, Throwable th) {
        super(msg, th);
    }
}
