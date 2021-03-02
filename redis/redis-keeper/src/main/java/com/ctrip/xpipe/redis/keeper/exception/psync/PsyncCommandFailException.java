package com.ctrip.xpipe.redis.keeper.exception.psync;

import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;

/**
 * @author Slight
 *
 *         Mar 02, 2021 5:18 PM
 */
public class PsyncCommandFailException extends PsyncRuntimeException {

    public PsyncCommandFailException(Throwable th) {
        super("[psync] command fail", th);
    }

    @Override
    public PsyncFailReason toReason() {
        return PsyncFailReason.PSYNC_COMMAND_FAIL;
    }
}
