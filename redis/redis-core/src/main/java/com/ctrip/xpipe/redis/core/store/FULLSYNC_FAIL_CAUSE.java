package com.ctrip.xpipe.redis.core.store;

/**
 * @author lishanglin
 * date 2022/6/7
 */
public enum FULLSYNC_FAIL_CAUSE {

    MISS_CMD_AFTER_RDB,
    TOO_MUCH_CMD_AFTER_RDB,
    RDB_NOT_OK,
    RDB_GTIDSET_NOT_READY

}
