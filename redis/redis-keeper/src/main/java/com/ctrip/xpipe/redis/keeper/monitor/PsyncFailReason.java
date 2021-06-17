package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncRuntimeException;

/**
 * @author chen.zhu
 * <p>
 * Mar 06, 2020
 */
public enum PsyncFailReason {

    OTHER,
    TOKEN_LACK,
    MASTER_RDB_OFFSET_NOT_CONTINUOUS,
    CONNECT_MASTER_FAIL,
    PSYNC_COMMAND_FAIL,
    PSYNC_REPL_ID_NOT_SAME,
    MASTER_DISCONNECTED;

    public static PsyncFailReason from(Throwable th) {
        if (th instanceof PsyncRuntimeException) {
            ((PsyncRuntimeException) th).toReason();
        }
        return OTHER;
    }
}
