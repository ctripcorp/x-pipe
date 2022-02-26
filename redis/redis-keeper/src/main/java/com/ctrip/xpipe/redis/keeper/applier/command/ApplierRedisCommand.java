package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

import java.util.List;

/**
 * @author Slight
 *
 * Jan 29, 2022 4:08 PM
 */
public interface ApplierRedisCommand<V> extends Command<V> {

    enum ApplierRedisCommandType {
        SINGLE_KEY,
        MULTI_KEY,
        MULTI,
        EXEC,
        UNKNOWN,
    }

    RedisOp redisOp();

    default ApplierRedisCommandType type() {
        RedisOp op = redisOp();
        if (op instanceof RedisSingleKeyOp) {
            return ApplierRedisCommandType.SINGLE_KEY;
        }
        if (op instanceof RedisMultiKeyOp) {
            return ApplierRedisCommandType.MULTI_KEY;
        }
        return ApplierRedisCommandType.UNKNOWN;
    }

    default RedisKey key() {
        RedisOp op = redisOp();
        if (op instanceof RedisSingleKeyOp) {
            return ((RedisSingleKeyOp<?>) op).getKey();
        }
        if (op instanceof RedisMultiKeyOp) {
            return keys().get(0);
        }
        throw new UnsupportedOperationException("key() not on RedisSingleKeyOp");
    }

    default List<RedisKey> keys() {
        RedisOp op = redisOp();
        if (op instanceof RedisMultiKeyOp) {
            return ((RedisMultiKeyOp<?>) op).getKeys();
        }
        throw new UnsupportedOperationException("keys() not on RedisMultiKeyOp");
    }

    @Override
    default String getName() {
        RedisOp op = redisOp();
        return op.getOpType().name() + ":" + op.getOpGtid();
    }
}
