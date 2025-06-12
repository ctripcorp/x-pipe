package com.ctrip.xpipe.redis.keeper.exception.psync;

import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;

public class GapAllowedSyncRdbNotContinuousRuntimeException extends PsyncRuntimeException {

    public GapAllowedSyncRdbNotContinuousRuntimeException(String reason) {
        super("[gasync] rdb not continuous: " + reason);
    }

    @Override
    public PsyncFailReason toReason() {
        return PsyncFailReason.MASTER_RDB_OFFSET_NOT_CONTINUOUS;
    }
}
