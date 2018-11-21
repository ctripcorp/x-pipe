package com.ctrip.xpipe.redis.core.proxy.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.XpipeRuntimeException;

public class XPipeProxyResultException extends XpipeRuntimeException {

    public XPipeProxyResultException(String message) {
        super(message);
    }

    public XPipeProxyResultException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> XPipeProxyResultException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
