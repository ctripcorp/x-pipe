package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author Slight
 *
 * Jan 29, 2022 4:08 PM
 */
public interface ApplierRedisCommand<V> extends Command<V> {

    RedisOp redisOp();

    default List<RedisKey> keys() {
        RedisOp op = redisOp();
        if (op instanceof RedisMultiKeyOp) {
            return ((RedisMultiKeyOp<?>) op).getKeys();
        }
        if (op instanceof RedisSingleKeyOp) {
            RedisKey key = ((RedisSingleKeyOp<?>) op).getKey();
            return Lists.newArrayList(key);
        }
        return Lists.newArrayList();
    }
}
