package com.ctrip.xpipe.redis.core.redis.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author lishanglin
 * date 2022/6/17
 */
public class IntsetParseFailException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public IntsetParseFailException(String message) {
        super(message);
    }

    public IntsetParseFailException(String msg, Throwable th) {
        super(msg, th);
    }

}
