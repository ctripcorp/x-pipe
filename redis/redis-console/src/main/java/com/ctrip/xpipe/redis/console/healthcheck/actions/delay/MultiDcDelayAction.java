package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class MultiDcDelayAction extends DelayAction {

    public MultiDcDelayAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                       ExecutorService executors, PingService pingService) {
        super(scheduled, instance, executors, pingService);
    }

    @Override
    protected void doSubscribe(RedisSession session, String channel, SubscribeCallback callback) {
        session.crdtsubscribeIfAbsent(channel, callback);
    }

    @Override
    protected void doPublish(RedisSession session, String channel, String message) {
        session.crdtpublish(channel, message);
    }

}
