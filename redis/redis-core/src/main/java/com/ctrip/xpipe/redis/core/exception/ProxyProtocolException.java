package com.ctrip.xpipe.redis.core.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author chen.zhu
 * <p>
 * May 11, 2018
 */
public class ProxyProtocolException extends XpipeRuntimeException {

    public ProxyProtocolException(String message) {
        super(message);
    }

    public ProxyProtocolException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> ProxyProtocolException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
