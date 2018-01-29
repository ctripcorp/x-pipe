package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 16, 2018
 */
public class RedisKeeperServerStateException extends RedisKeeperRuntimeException{

    public RedisKeeperServerStateException(String keeperDesc, String currentLifecycle) {
        super(String.format("keeper:%s, currentState:%s", keeperDesc, currentLifecycle));
    }
}
