package com.ctrip.xpipe.redis.keeper.exception.psync;

import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;

/**
 * @author Slight
 * <p>
 * Jun 10, 2021 5:42 PM
 */
public class RdbOnlyPsyncReplIdNotSameException extends PsyncRuntimeException {

    public RdbOnlyPsyncReplIdNotSameException(String message, Throwable th) {
        super(message, th);
    }

    @Override
    public PsyncFailReason toReason() {
        return PsyncFailReason.PSYNC_REPL_ID_NOT_SAME;
    }
}
