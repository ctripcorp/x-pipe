package com.ctrip.xpipe.redis.proxy.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class WriteWhenSessionInitException extends XpipeRuntimeException {

    public WriteWhenSessionInitException(String message) {
        super(message);
    }

    public WriteWhenSessionInitException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> WriteWhenSessionInitException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
