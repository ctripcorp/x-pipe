package com.ctrip.xpipe.redis.console.exception;

/**
 * http code:500
 *
 * @Author zhangle
 */
public class ServerException extends RedisConsoleRuntimeException{
    private static final long serialVersionUID = 1L;

    public ServerException(String message) {
        super(message);
    }

    public ServerException(String msg, Throwable th) {
        super(msg, th);
    }
}
