package com.ctrip.xpipe.redis.keeper.exception.psync;

import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;

/**
 * @author Slight
 *
 *         Mar 02, 2021 4:50 PM
 */
public class PsyncMasterRdbOffsetNotContinuousRuntimeException extends PsyncRuntimeException {

    public PsyncMasterRdbOffsetNotContinuousRuntimeException(long masterRdbOffset, long firstAvailable) {
        super("[psync] master rdb offset not continuous: masterRdbOffset: " + masterRdbOffset + " firstAvailable: " + firstAvailable);
    }

    @Override
    public PsyncFailReason toReason() {
        return PsyncFailReason.MASTER_RDB_OFFSET_NOT_CONTINUOUS;
    }
}
