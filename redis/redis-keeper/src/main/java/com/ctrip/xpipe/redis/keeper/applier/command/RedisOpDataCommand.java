package com.ctrip.xpipe.redis.keeper.applier.command;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author Slight
 * <p>
 * Mar 01, 2022 8:55 AM
 */
public interface RedisOpDataCommand<V> extends RedisOpCommand<V> {

    default List<RedisOpCommand<V>> sharding() {
        return Lists.newArrayList(this);
    }
}
