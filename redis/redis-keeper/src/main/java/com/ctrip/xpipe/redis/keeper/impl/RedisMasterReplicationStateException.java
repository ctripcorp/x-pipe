package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 23, 2018
 */
public class RedisMasterReplicationStateException extends RedisKeeperRuntimeException {

    public RedisMasterReplicationStateException(RedisMasterReplication redisMasterReplication, String message) {
        super(String.format("%s, %s", redisMasterReplication, message));
    }
}
