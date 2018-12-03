package com.ctrip.xpipe.redis.core.proxy.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

public class ParseException extends XpipeRuntimeException {
    public ParseException(String message) {
        super(message);
    }
}
