package com.ctrip.xpipe.redis.core.store;

/**
 * @author lishanglin
 * date 2022/4/15
 */
public interface ReplicationProgress<T> {

    T getProgress();

}
