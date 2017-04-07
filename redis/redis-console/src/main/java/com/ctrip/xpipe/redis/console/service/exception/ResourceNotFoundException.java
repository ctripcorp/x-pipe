package com.ctrip.xpipe.redis.console.service.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 07, 2017
 */
public class ResourceNotFoundException extends RedisConsoleException{

    public ResourceNotFoundException(String message) {
        super(message);
    }

    public ResourceNotFoundException(String dc, String clusterId, String shardId){
        this(String.format("not found: dc:%s, cluster:%s, shard:%s", dc, clusterId, shardId));
    }

}
