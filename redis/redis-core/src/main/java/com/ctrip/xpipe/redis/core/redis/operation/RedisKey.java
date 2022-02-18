package com.ctrip.xpipe.redis.core.redis.operation;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisKey {

    private String key;

    private String vectorClock;

    private Long expire;

    public RedisKey(String key) {
        this(key, null, null);
    }

    public RedisKey(String key, Long expire) {
        this(key, expire, null);
    }

    public RedisKey(String key, String vectorClock) {
        this(key, null, vectorClock);
    }

    public RedisKey(String key, Long expire, String vectorClock) {
        this.key = key;
        this.expire = expire;
        this.vectorClock = vectorClock;
    }

    public String get() {
        return key;
    }

    public String getVectorClock() {
        return vectorClock;
    }

    public Long getExpire() {
        return expire;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedisKey redisKey = (RedisKey) o;
        return key.equals(redisKey.key) &&
                Objects.equals(vectorClock, redisKey.vectorClock) &&
                Objects.equals(expire, redisKey.expire);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, vectorClock, expire);
    }
}
