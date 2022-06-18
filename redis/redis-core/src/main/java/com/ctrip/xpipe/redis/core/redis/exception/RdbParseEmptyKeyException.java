package com.ctrip.xpipe.redis.core.redis.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author lishanglin
 * date 2022/6/17
 */
public class RdbParseEmptyKeyException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public RdbParseEmptyKeyException(String message) {
        super(message);
    }

    public RdbParseEmptyKeyException(String msg, Throwable th) {
        super(msg, th);
    }

}
