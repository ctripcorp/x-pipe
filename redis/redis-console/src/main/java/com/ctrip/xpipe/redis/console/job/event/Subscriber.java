package com.ctrip.xpipe.redis.console.job.event;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface Subscriber<T> {

    void register(EventBus eventBus);

    void unregister(EventBus eventBus);

    void processData(T t);
}
