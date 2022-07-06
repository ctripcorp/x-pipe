package com.ctrip.xpipe.redis.core.redis.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;

/**
 * @author lishanglin
 * date 2022/6/22
 */
public class RdbStreamParseFailException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public RdbStreamParseFailException(String message) {
        super(message);
    }

    public RdbStreamParseFailException(RedisKey key, String message) {
        this(null != key ? "key:" + key + " " + message : message);
    }

    public RdbStreamParseFailException(String msg, Throwable th) {
        super(msg, th);
    }

}
