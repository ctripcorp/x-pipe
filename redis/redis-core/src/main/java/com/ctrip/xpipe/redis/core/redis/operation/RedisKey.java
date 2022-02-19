package com.ctrip.xpipe.redis.core.redis.operation;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisKey {

    private String key;

    private String vectorClock;

    public RedisKey(String key) {
        this(key, null);
    }

    public RedisKey(String key, String vectorClock) {
        this.key = key;
        this.vectorClock = vectorClock;
    }

    public String get() {
        return key;
    }

    public String getVectorClock() {
        return vectorClock;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedisKey key1 = (RedisKey) o;
        return key.equals(key1.key) &&
                Objects.equals(vectorClock, key1.vectorClock);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, vectorClock);
    }
}
