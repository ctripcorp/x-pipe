package com.ctrip.xpipe.redis.console.exception;

public class DcMetaBuilderException extends RedisConsoleRuntimeException {

    private static final long serialVersionUID = 1L;

    public DcMetaBuilderException(String message) {
        super(message);
    }

    public DcMetaBuilderException(String message, Throwable cause) {
        super(message, cause);
    }
}
