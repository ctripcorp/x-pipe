package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpExec;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMulti;

import java.util.List;

/**
 * @author Slight
 *
 * Jan 29, 2022 4:08 PM
 */
public interface RedisOpCommand<V> extends Command<V> {

    enum RedisOpCommandType {
        SINGLE_KEY,
        MULTI_KEY,
        MULTI,
        EXEC,
        UNKNOWN,
    }

    RedisOp redisOp();

    default String gtid() {
        return redisOp().getOpGtid();
    }

    default RedisSingleKeyOp<V> redisOpAsSingle() {
        RedisOp op = redisOp();
        if (op instanceof RedisSingleKeyOp) {
            return (RedisSingleKeyOp<V>) op;
        }
        throw new XpipeRuntimeException("invalid type of RedisOp");
    }

    default RedisMultiKeyOp<V> redisOpAsMulti() {
        RedisOp op = redisOp();
        if (op instanceof RedisMultiKeyOp) {
            return (RedisMultiKeyOp<V>) op;
        }
        throw new XpipeRuntimeException("invalid type of RedisOp");
    }

    default RedisOpCommandType type() {
        RedisOp op = redisOp();
        if (op instanceof RedisOpMulti) {
            return RedisOpCommandType.MULTI;
        }
        if (op instanceof RedisOpExec) {
            return RedisOpCommandType.EXEC;
        }
        if (op instanceof RedisSingleKeyOp) {
            return RedisOpCommandType.SINGLE_KEY;
        }
        if (op instanceof RedisMultiKeyOp) {
            return RedisOpCommandType.MULTI_KEY;
        }
        return RedisOpCommandType.UNKNOWN;
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
