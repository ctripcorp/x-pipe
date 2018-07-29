package com.ctrip.xpipe.redis.proxy.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author chen.zhu
 * <p>
 * May 12, 2018
 */
public class ResourceNotFoundException extends XpipeRuntimeException {
    public ResourceNotFoundException(String message) {
        super(message);
    }

    public ResourceNotFoundException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> ResourceNotFoundException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
