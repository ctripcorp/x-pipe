package com.ctrip.xpipe.redis.core.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author chen.zhu
 * <p>
 * May 31, 2018
 */
public class NoResourceException extends XpipeRuntimeException {
    public NoResourceException(String message) {
        super(message);
    }

    public NoResourceException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> NoResourceException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
