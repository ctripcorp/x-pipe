package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;

/**
 * @author ayq
 * <p>
 * 2022/6/8 15:21
 */
public enum RedisOpMultiKVEnum {
    MSET(RedisOpType.MSET, 1, 2),
    MSETNX(RedisOpType.MSETNX, 1, 2),
    DEL(RedisOpType.DEL, 1, 1);

    RedisOpMultiKVEnum(RedisOpType redisOpType, int keyStartIndex, int kvNum) {
        this.redisOpType = redisOpType;
        this.keyStartIndex = keyStartIndex;
        this.kvNum = kvNum;
    }

    public static RedisOpMultiKVEnum findByRedisOpType(RedisOpType redisOpType) {
        for (RedisOpMultiKVEnum multiKVEnum: RedisOpMultiKVEnum.values()) {
            if (multiKVEnum.getRedisOpType().equals(redisOpType)) {
                return multiKVEnum;
            }
        }

        throw new IllegalArgumentException("RedisOpType not found in RedisOpMultiKVEnum: " + redisOpType.name());
    }

    private RedisOpType redisOpType;
    private int keyStartIndex;
    private int kvNum;

    public RedisOpType getRedisOpType() {
        return redisOpType;
    }

    public int getKeyStartIndex() {
        return keyStartIndex;
    }

    public int getKvNum() {
        return kvNum;
    }
}
