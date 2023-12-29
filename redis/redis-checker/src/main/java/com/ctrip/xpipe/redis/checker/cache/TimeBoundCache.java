package com.ctrip.xpipe.redis.checker.cache;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class TimeBoundCache<T> {

    private T data;

    private long expiredAt;

    private LongSupplier timeoutMillSupplier;

    private Supplier<T> dataSupplier;

    public TimeBoundCache(LongSupplier timeoutMillSupplier, Supplier<T> dataSupplier) {
        this.data = null;
        this.expiredAt = 0L;
        this.timeoutMillSupplier = timeoutMillSupplier;
        this.dataSupplier = dataSupplier;
    }

    // allow data timeout
    public T getCurrentData() {
        return this.data;
    }

    public T getData() {
        return getData(false);
    }

    public T getData(boolean disableCache) {
        if (!disableCache && null != data && expiredAt > System.currentTimeMillis()) {
            return data;
        }

        synchronized (this) {
            if (!disableCache && null != data && expiredAt > System.currentTimeMillis()) return data;
            this.data = dataSupplier.get();
            this.expiredAt = System.currentTimeMillis() + timeoutMillSupplier.getAsLong();
            return this.data;
        }
    }

    public synchronized void refresh() {
        this.data = dataSupplier.get();
        this.expiredAt = System.currentTimeMillis() + timeoutMillSupplier.getAsLong();
    }

}
