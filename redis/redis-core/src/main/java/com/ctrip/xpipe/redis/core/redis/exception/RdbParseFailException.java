package com.ctrip.xpipe.redis.core.redis.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;

/**
 * @author lishanglin
 * date 2022/6/19
 */
public class RdbParseFailException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public RdbParseFailException(RedisKey key, String message) {
        this(null != key ? "key:" + key + " " + message : message);
    }

    public RdbParseFailException(String message) {
        super(message);
    }

    public RdbParseFailException(String msg, Throwable th) {
        super(msg, th);
    }

}
