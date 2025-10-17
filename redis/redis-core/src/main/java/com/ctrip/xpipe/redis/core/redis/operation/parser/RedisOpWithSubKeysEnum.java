package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;

/**
 * @author tb
 * <p>
 * 2025/10/10 15:21
 */
public enum RedisOpWithSubKeysEnum {

    //hash
    HDEL(RedisOpType.HDEL, 1, 2,false),
    HINCRBY(RedisOpType.HINCRBY, 1, 2,false),
    HINCRBYFLOAT(RedisOpType.HINCRBYFLOAT, 1, 2,false),
    HMSET(RedisOpType.HMSET, 1, 2,false),
    HSET(RedisOpType.HSET, 1, 2,false),
    HSETNX(RedisOpType.HSETNX, 1, 2,false),

    ZADD(RedisOpType.ZADD, 1, 2,true),
    ZREM(RedisOpType.ZREM, 1, 1,false),
    ZINCRBY(RedisOpType.ZINCRBY, 1, 2,true),


    GEOADD(RedisOpType.GEOADD, 1, 3,true),


    SREM(RedisOpType.SREM, 1, 1,false),
    SADD(RedisOpType.SADD, 1, 1,false);




    RedisOpWithSubKeysEnum(RedisOpType redisOpType, int keyStartIndex, int kvNum,boolean kvReverse) {
        this.redisOpType = redisOpType;
        this.keyStartIndex = keyStartIndex;
        this.kvNum = kvNum;
        this.kvReverse = kvReverse;
    }

    public static RedisOpWithSubKeysEnum findByRedisOpType(RedisOpType redisOpType) {
        for (RedisOpWithSubKeysEnum multiKVEnum: RedisOpWithSubKeysEnum.values()) {
            if (multiKVEnum.getRedisOpType().equals(redisOpType)) {
                return multiKVEnum;
            }
        }

        throw new IllegalArgumentException("RedisOpType not found in RedisOpMultiKVEnum: " + redisOpType.name());
    }

    private RedisOpType redisOpType;
    private int keyStartIndex;
    private int kvNum;
    private boolean kvReverse;

    public RedisOpType getRedisOpType() {
        return redisOpType;
    }

    public int getKeyStartIndex() {
        return keyStartIndex;
    }

    public int getKvNum() {
        return kvNum;
    }

    public boolean getKvReverse() {
        return this.kvReverse;
    }
}
