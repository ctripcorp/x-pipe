package com.ctrip.xpipe.redis.keeper.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;

/**
 * @author wenchao.meng
 *
 * Jun 22, 2016
 */
public class RedisKeeperRuntimeException extends RedisRuntimeException {

    private static final long serialVersionUID = 1L;

    public RedisKeeperRuntimeException(String message) {
        super(message);
    }

    public RedisKeeperRuntimeException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> RedisKeeperRuntimeException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
