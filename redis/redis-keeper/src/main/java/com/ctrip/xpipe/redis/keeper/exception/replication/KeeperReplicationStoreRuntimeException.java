package com.ctrip.xpipe.redis.keeper.exception.replication;

import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;

/**
 * @author Slight
 * <p>
 * Jun 10, 2021 5:14 PM
 */
public class KeeperReplicationStoreRuntimeException extends RedisKeeperRuntimeException {

    public KeeperReplicationStoreRuntimeException(String message) {
        super(message);
    }
}
