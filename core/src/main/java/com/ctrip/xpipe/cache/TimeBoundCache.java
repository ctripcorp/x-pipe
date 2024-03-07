package com.ctrip.xpipe.cache;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class TimeBoundCache<T> {

    private T data;

    private long lastRefreshAt;

    private long expiredAt;

    private LongSupplier timeoutMillSupplier;

    private Supplier<T> dataSupplier;

    public TimeBoundCache(LongSupplier timeoutMillSupplier, Supplier<T> dataSupplier) {
        this.data = null;
        this.expiredAt = 0L;
        this.lastRefreshAt = 0L;
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
        long current = System.currentTimeMillis();
        if (current < lastRefreshAt) {
            // system time roll back
            resetExpireAt();
        }

        if (!disableCache && null != data && expiredAt > current) {
            return data;
        }

        synchronized (this) {
            if (!disableCache && null != data && expiredAt > current) return data;
            data = dataSupplier.get();
            refreshExpireAt();
            return data;
        }
    }

    public synchronized void refresh() {
        this.data = dataSupplier.get();
        refreshExpireAt();
    }

    private void resetExpireAt() {
        this.lastRefreshAt = 0L;
        this.expiredAt = 0L;
    }

    private void refreshExpireAt() {
        long timeout = timeoutMillSupplier.getAsLong();
        this.lastRefreshAt = System.currentTimeMillis();
        this.expiredAt = lastRefreshAt + timeout;
        // expiredAt exceeds max long
        if (this.expiredAt < timeout) this.expiredAt = Long.MAX_VALUE;
    }

}
