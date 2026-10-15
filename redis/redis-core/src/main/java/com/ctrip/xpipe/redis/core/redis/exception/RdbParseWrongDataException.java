package com.ctrip.xpipe.redis.core.redis.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;

public class RdbParseWrongDataException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public RdbParseWrongDataException(RedisKey key, String message) {
        this(null != key ? "key:" + key + " " + message : message);
    }

    public RdbParseWrongDataException(String message) {
        super(message);
    }

    public RdbParseWrongDataException(String msg, Throwable th) {
        super(msg, th);
    }

}

