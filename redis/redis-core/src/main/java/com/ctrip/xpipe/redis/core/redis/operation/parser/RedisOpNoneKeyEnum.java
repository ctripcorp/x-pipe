package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;

/**
 * @author ayq
 * <p>
 * 2022/6/12 14:17
 */
public enum RedisOpNoneKeyEnum {

    SELECT(RedisOpType.SELECT),
    PING(RedisOpType.PING),
    MULT(RedisOpType.MULTI),
    EXEC(RedisOpType.EXEC),
    SCRIPT(RedisOpType.SCRIPT),

    UNKNOW(RedisOpType.UNKNOWN),

    //crdt
    CRDT_SELECT(RedisOpType.CRDT_SELECT);

    private RedisOpType redisOpType;

    RedisOpNoneKeyEnum(RedisOpType redisOpType) {
        this.redisOpType = redisOpType;
    }

    public RedisOpType getRedisOpType() {
        return redisOpType;
    }
}
