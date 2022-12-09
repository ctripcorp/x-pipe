package com.ctrip.xpipe.redis.core.keeper.applier.container;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;

/**
 * @author ayq
 * <p>
 * 2022/4/2 16:57
 */
public class ApplierContainerException extends RedisRuntimeException {

    public ApplierContainerException(String message) {
        super(message);
    }

    public ApplierContainerException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> ApplierContainerException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
