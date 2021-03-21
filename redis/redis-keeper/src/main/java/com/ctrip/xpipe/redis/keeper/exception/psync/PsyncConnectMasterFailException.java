package com.ctrip.xpipe.redis.keeper.exception.psync;

import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;

/**
 * @author Slight
 *
 *         Mar 02, 2021 4:59 PM
 */
public class PsyncConnectMasterFailException extends PsyncRuntimeException {

    public PsyncConnectMasterFailException(Throwable th) {
        super("[psync] fail to connect master", th);
    }

    @Override
    public PsyncFailReason toReason() {
        return PsyncFailReason.CONNECT_MASTER_FAIL;
    }
}
