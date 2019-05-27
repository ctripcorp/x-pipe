package com.ctrip.xpipe.redis.core.proxy.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

public class ProxyProtocolParseException extends XpipeRuntimeException {
    public ProxyProtocolParseException(String message) {
        super(message);
    }
}
