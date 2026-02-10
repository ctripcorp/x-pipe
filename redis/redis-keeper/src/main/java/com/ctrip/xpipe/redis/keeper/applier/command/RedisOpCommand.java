package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.*;

import java.util.List;

/**
 * @author Slight
 *
 * Jan 29, 2022 4:08 PM
 */
public interface RedisOpCommand<V> extends Command<V> {

    RedisOp redisOp();

    default String gtid() {
        return redisOp().getOpGtid();
    }

    default boolean needGuaranteeSuccess() {return true;}

    default RedisSingleKeyOp redisOpAsSingle() {
        RedisOp op = redisOp();
        if (op instanceof RedisSingleKeyOp) {
            return (RedisSingleKeyOp) op;
        }
        throw new XpipeRuntimeException("invalid type of RedisOp");
    }

    default RedisMultiKeyOp redisOpAsMulti() {
        RedisOp op = redisOp();
        if (op instanceof RedisMultiKeyOp) {
            return (RedisMultiKeyOp) op;
        }
        throw new XpipeRuntimeException("invalid type of RedisOp");
    }

    default RedisOpCommandType type() {
        RedisOp op = redisOp();
        if (op instanceof RedisSingleKeyOp || op instanceof RedisMultiSubKeyOp) {
            return RedisOpCommandType.SINGLE_KEY;
        }
        if (op instanceof RedisMultiKeyOp) {
            return RedisOpCommandType.MULTI_KEY;
        }
        return RedisOpCommandType.OTHER;
    }

    default RedisKey key() {
        RedisOp op = redisOp();
        if (op instanceof RedisSingleKeyOp) {
            return ((RedisSingleKeyOp) op).getKey();
        }
        if (op instanceof RedisMultiKeyOp) {
            return keys().get(0);
        }
        if(op instanceof RedisMultiSubKeyOp){
            return ((RedisMultiSubKeyOp)op).getKey();
        }
        throw new UnsupportedOperationException("key() not on RedisSingleKeyOp");
    }

    default List<RedisKey> keys() {
        RedisOp op = redisOp();
        if (op instanceof RedisMultiKeyOp) {
            return ((RedisMultiKeyOp) op).getKeys();
        }
        throw new UnsupportedOperationException("keys() not on RedisMultiKeyOp");
    }

    @Override
    default String getName() {
        RedisOp op = redisOp();
        return op.getOpType().name() + ":" + op.getOpGtid();
    }
}
