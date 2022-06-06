package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;

/**
 * @author ayq
 * <p>
 * 2022/6/7 12:02
 */
public enum RedisOpSingleKVEnum {
    SET(RedisOpType.SET, 1, 2),
    SETNX(RedisOpType.SETNX, 1, 2),
    SETEX(RedisOpType.SETEX, 1, 2),
    INCRBY(RedisOpType.INCRBY, 1, 2),
    DECRBY(RedisOpType.DECRBY, 1, 2),
    PUBLISH(RedisOpType.PUBLISH, 1, 2),
    PSETEX(RedisOpType.PSETEX, 1, 3),
    INCR(RedisOpType.INCR, 1, null),
    DECR(RedisOpType.DECR, 1, null),
    SELECT(RedisOpType.SELECT, 1, null),
    PING(RedisOpType.PING, null, null),
    MULT(RedisOpType.MULTI, null, null),
    EXEC(RedisOpType.EXEC, null, null);

    RedisOpSingleKVEnum(RedisOpType redisOpType, Integer keyIndex, Integer valueIndex) {
        this.redisOpType = redisOpType;
        this.keyIndex = keyIndex;
        this.valueIndex = valueIndex;
    }

    private RedisOpType redisOpType;
    private Integer keyIndex;
    private Integer valueIndex;

    public RedisOpType getRedisOpType() {
        return redisOpType;
    }

    public Integer getKeyIndex() {
        return keyIndex;
    }

    public Integer getValueIndex() {
        return valueIndex;
    }
}
