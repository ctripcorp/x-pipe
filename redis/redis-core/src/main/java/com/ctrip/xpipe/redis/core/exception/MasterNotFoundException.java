package com.ctrip.xpipe.redis.core.exception;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 06, 2017
 */
public class MasterNotFoundException extends RedisException{

    public MasterNotFoundException(String clusterId, String shardId) {
        super(String.format("[cluster:%s, shard:%s]", clusterId, shardId));
    }
}
