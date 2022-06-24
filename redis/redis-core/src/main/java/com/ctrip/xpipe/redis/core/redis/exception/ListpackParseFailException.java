package com.ctrip.xpipe.redis.core.redis.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author lishanglin
 * date 2022/6/18
 */
public class ListpackParseFailException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public ListpackParseFailException(String message) {
        super(message);
    }

    public ListpackParseFailException(String msg, Throwable th) {
        super(msg, th);
    }

}
