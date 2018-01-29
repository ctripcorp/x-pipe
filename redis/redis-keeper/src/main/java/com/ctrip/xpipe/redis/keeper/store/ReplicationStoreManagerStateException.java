package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 16, 2018
 */
public class ReplicationStoreManagerStateException extends RedisKeeperRuntimeException{

    public ReplicationStoreManagerStateException(String errorMsg, String storeDesc, String currentLifecycle) {
        super(String.format("%s, %s, currentLifecycle:%s", errorMsg, storeDesc, currentLifecycle));
    }
}
