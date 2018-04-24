package com.ctrip.xpipe.redis.console.job.event;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface Dispatcher<T> {
    void dispatch(Subscriber<T> subscriber, T t);
}
