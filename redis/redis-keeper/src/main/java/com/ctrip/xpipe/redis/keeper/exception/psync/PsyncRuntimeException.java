package com.ctrip.xpipe.redis.keeper.exception.psync;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;

/**
 * @author Slight
 *
 *         Mar 02, 2021 4:49 PM
 */
public class PsyncRuntimeException extends RedisKeeperRuntimeException {

    public PsyncRuntimeException(String message) {
        super(message);
    }

    public PsyncRuntimeException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> PsyncRuntimeException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }

    public PsyncFailReason toReason() {
        return PsyncFailReason.OTHER;
    }
}
