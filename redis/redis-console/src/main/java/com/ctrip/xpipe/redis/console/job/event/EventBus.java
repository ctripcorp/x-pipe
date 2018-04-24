package com.ctrip.xpipe.redis.console.job.event;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface EventBus<T> {

    void post(T t);

    void register(Subscriber subscriber);

    void unregister(Subscriber subscriber);

    void dispatcher(Dispatcher<T> dispatcher);
}
