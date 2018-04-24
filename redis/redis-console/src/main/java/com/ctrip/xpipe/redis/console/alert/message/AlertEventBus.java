package com.ctrip.xpipe.redis.console.alert.message;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.job.event.Dispatcher;
import com.ctrip.xpipe.redis.console.job.event.EventBus;
import com.ctrip.xpipe.redis.console.job.event.Subscriber;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.concurrent.Executor;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */

public class AlertEventBus implements EventBus<AlertEntity> {

    private Set<AlertEntitySubscriber> alertEntitySubscribers = Sets.newHashSet();

    private Dispatcher dispatcher;

    public AlertEventBus(Executor executors) {
        dispatcher = new SingleThreadDispatcher(executors);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void post(AlertEntity alertEntity) {
        for(AlertEntitySubscriber subscriber : alertEntitySubscribers) {
            dispatcher.dispatch(subscriber, alertEntity);
        }
    }

    @Override
    public void register(Subscriber subscriber) {
        if(subscriber instanceof AlertEntitySubscriber) {
            alertEntitySubscribers.add((AlertEntitySubscriber)subscriber);
        }
    }

    @Override
    public void unregister(Subscriber subscriber) {
        if(subscriber instanceof AlertEntitySubscriber) {
            alertEntitySubscribers.remove(subscriber);
        }
    }

    @Override
    public void dispatcher(Dispatcher<AlertEntity> dispatcher) {
        this.dispatcher = dispatcher;
    }
}
