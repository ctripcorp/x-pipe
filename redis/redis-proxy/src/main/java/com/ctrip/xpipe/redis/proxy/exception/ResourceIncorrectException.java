package com.ctrip.xpipe.redis.proxy.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public class ResourceIncorrectException extends XpipeRuntimeException {
    public ResourceIncorrectException(String message) {
        super(message);
    }

    public ResourceIncorrectException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> ResourceIncorrectException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
