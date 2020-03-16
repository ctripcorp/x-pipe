package com.ctrip.xpipe.redis.keeper.ratelimit;

/**
 * @author chen.zhu
 * <p>
 * Feb 25, 2020
 */
public interface LeakyBucket {

    boolean tryAcquire();

    void release();

    void resize(int newSize);

    int references();

    int getTotalSize();
}
