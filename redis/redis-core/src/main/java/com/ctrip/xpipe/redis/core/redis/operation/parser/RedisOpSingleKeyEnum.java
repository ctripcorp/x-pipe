package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;

/**
 * @author ayq
 * <p>
 * 2022/6/7 12:02
 */
public enum RedisOpSingleKeyEnum {

    // String
    SET(RedisOpType.SET, 1, 2),
    SETNX(RedisOpType.SETNX, 1, 2),
    SETEX(RedisOpType.SETEX, 1, 2),
    PSETEX(RedisOpType.PSETEX, 1, 3),
    GETSET(RedisOpType.GETSET, 1, null),
    INCR(RedisOpType.INCR, 1, null),
    DECR(RedisOpType.DECR, 1, null),
    INCRBY(RedisOpType.INCRBY, 1, 2),
    INCRBYFLOAT(RedisOpType.INCRBYFLOAT, 1, 2),
    DECRBY(RedisOpType.DECRBY, 1, 2),
    APPEND(RedisOpType.APPEND, 1, 2),
    SETRANGE(RedisOpType.SETRANGE, 1, 3),

    // List
    LPOP(RedisOpType.LPOP, 1, null),
    RPOP(RedisOpType.RPOP, 1, null),
    LINSERT(RedisOpType.LINSERT, 1, 3),
    LPUSH(RedisOpType.LPUSH, 1, 2),
    LPUSHX(RedisOpType.LPUSHX, 1, 2),
    RPUSH(RedisOpType.RPUSH, 1, 2),
    RPUSHX(RedisOpType.RPUSHX, 1, 2),
    LREM(RedisOpType.LREM, 1, 3),
    LSET(RedisOpType.LSET, 1, 3),
    LTRIM(RedisOpType.LTRIM, 1, null),

    // Hash
    HDEL(RedisOpType.HDEL, 1, 2),
    HINCRBY(RedisOpType.HINCRBY, 1, 2),
    HINCRBYFLOAT(RedisOpType.HINCRBYFLOAT, 1, 2),
    HMSET(RedisOpType.HMSET, 1, 2),
    HSET(RedisOpType.HSET, 1, 2),
    HSETNX(RedisOpType.HSETNX, 1, 2),


    // Set
    SADD(RedisOpType.SADD, 1, 2),
    SPOP(RedisOpType.SPOP, 1, 2),
    SREM(RedisOpType.SREM, 1, 2),

    // ZSet
    ZADD(RedisOpType.ZADD, 1, null),
    ZINCRBY(RedisOpType.ZINCRBY, 1, null),
    ZREM(RedisOpType.ZREM, 1, null),
    ZREMRANGEBYLEX(RedisOpType.ZREMRANGEBYLEX, 1, null),
    ZREMRANGEBYRANK(RedisOpType.ZREMRANGEBYRANK, 1, null),
    ZREMRANGEBYSCORE(RedisOpType.ZREMRANGEBYSCORE, 1, null),

    // TTL
    EXPIRE(RedisOpType.EXPIRE, 1, 2),
    EXPIREAT(RedisOpType.EXPIREAT, 1, 2),
    PEXPIRE(RedisOpType.PEXPIRE, 1, 2),
    PEXPIREAT(RedisOpType.PEXPIREAT, 1, 2),
    PERSIST(RedisOpType.PERSIST, 1, null),

    // Geo
    GEOADD(RedisOpType.GEOADD, 1, 2),
    GEORADIUS(RedisOpType.GEORADIUS, 1, 2),

    // Bit
    SETBIT(RedisOpType.SETBIT, 1, 3),

    // others
    PUBLISH(RedisOpType.PUBLISH, 1, 2),
    MOVE(RedisOpType.MOVE, 1, null);

    RedisOpSingleKeyEnum(RedisOpType redisOpType, Integer keyIndex, Integer valueIndex) {
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
