package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 06, 2017
 */
public class MasterNotFoundException extends RedisConsoleException{

    public MasterNotFoundException(String clusterId, String shardId) {
        super(String.format("[cluster:%s, shard:%s]", clusterId, shardId));
    }
}
