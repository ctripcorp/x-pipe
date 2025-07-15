package com.ctrip.xpipe.redis.keeper.exception.replication;

import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;

public class LostGtidsetBacklogConflictException extends RedisKeeperRuntimeException {

    public LostGtidsetBacklogConflictException(String message) {
        super(message);
    }
}
