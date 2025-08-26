package com.ctrip.xpipe.redis.core.store;

public enum UPDATE_RDB_RESULT {
    OK,
    REPLSTAGE_NOT_MATCH,
    REPLID_NOT_MATCH,
    LACK_BACKLOG,
    RDB_MORE_RECENT,
    REPLOFF_OUT_RANGE,
    MASTER_UUID_NOT_MATCH,
    GTID_SET_NOT_MATCH,
}

