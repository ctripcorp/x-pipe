package com.ctrip.xpipe.service.client.redis;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;

import java.util.List;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 2:37 PM
 */
public class CRedisAsyncClient implements AsyncRedisClient {

    @Override
    public Object select(Object key) {
        return null;
    }

    @Override
    public Map<Object, List<Object>> selectMulti(List<Object> keys) {
        return null;
    }

    @Override
    public CommandFuture<Object> write(Object shard, List<Object> rawArgs) {
        return null;
    }
}
