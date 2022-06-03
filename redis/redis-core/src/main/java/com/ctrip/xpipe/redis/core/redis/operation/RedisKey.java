package com.ctrip.xpipe.redis.core.redis.operation;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisKey {

    private byte[] key;

    private String vectorClock;

    public RedisKey(byte[] key) {
        this(key, null);
    }

    public RedisKey(String key) {
        this(key.getBytes(), null);
    }

    public RedisKey(byte[] key, String vectorClock) {
        this.key = key;
        this.vectorClock = vectorClock;
    }

    public byte[] get() {
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
        return Arrays.equals(key, key1.key) &&
                Objects.equals(vectorClock, key1.vectorClock);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(vectorClock);
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }
}
