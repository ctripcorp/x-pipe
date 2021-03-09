package com.ctrip.xpipe.redis.checker.alert.event;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface Dispatcher<T> {
    void dispatch(Subscriber<T> subscriber, T t);
}
