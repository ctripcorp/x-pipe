package com.ctrip.xpipe.redis.core.redis.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author lishanglin
 * date 2022/6/17
 */
public class ZiplistParseFailException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public ZiplistParseFailException(String message) {
        super(message);
    }

    public ZiplistParseFailException(String msg, Throwable th) {
        super(msg, th);
    }

}
