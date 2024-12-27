package com.ctrip.xpipe.redis.keeper.exception.psync;

import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;

public class KeeperTolerantClosePsyncException extends PsyncRuntimeException {

    public KeeperTolerantClosePsyncException(PsyncRuntimeException e) {
        super("keeper tolerant:" + e.getMessage(), e);
    }

    @Override
    public PsyncFailReason toReason() {
        Throwable cause = getCause();
        if (cause instanceof PsyncRuntimeException) return ((PsyncRuntimeException) cause).toReason();
        else return super.toReason();
    }

}
