package com.ctrip.xpipe.redis.keeper.applier.command;

import java.util.List;

/**
 * @author Slight
 * <p>
 * Mar 01, 2022 8:55 AM
 */
public interface ApplierRedisOpCommand<V> extends RedisOpCommand<V> {

    List<RedisOpCommand<V>> sharding();
}
