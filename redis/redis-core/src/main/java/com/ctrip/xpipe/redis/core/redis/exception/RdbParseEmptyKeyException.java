package com.ctrip.xpipe.redis.core.redis.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;

/**
 * @author lishanglin
 * date 2022/6/17
 */
public class RdbParseEmptyKeyException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public RdbParseEmptyKeyException(RedisKey key, String message) {
        this(null != key ? "key:" + key + " " + message : message);
    }

    public RdbParseEmptyKeyException(String message) {
        super(message);
    }

    public RdbParseEmptyKeyException(String msg, Throwable th) {
        super(msg, th);
    }

}
