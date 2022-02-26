package com.ctrip.xpipe.client.redis;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.utils.ServicesUtil;

import java.util.List;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:14 PM
 */
public interface AsyncRedisClient {

    Object /* shard */ select(Object key);

    Map<Object /* shard */, List<Object> /* keys */> selectMulti(List<Object> keys);

    CommandFuture<Object> write(Object shard, List<Object> rawArgs);
}
