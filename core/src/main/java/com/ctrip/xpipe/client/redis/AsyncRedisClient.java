package com.ctrip.xpipe.client.redis;

import com.ctrip.xpipe.api.command.CommandFuture;

import java.util.List;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:14 PM
 */
public interface AsyncRedisClient {

    Object[] /* resources */ broadcast();

    Object /* resource */ select(Object key);

    Map<Object /* resource */, List<Object> /* keys */> selectMulti(List<Object> keys);

    CommandFuture<Object> write(Object resource, Object... rawArgs);

    CommandFuture<Object> multi();

    CommandFuture<Object> exec(Object... rawArgs);
}
