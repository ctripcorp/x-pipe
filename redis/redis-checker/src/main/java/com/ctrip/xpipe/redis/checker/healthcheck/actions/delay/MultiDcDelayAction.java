package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class MultiDcDelayAction extends DelayAction {

    public MultiDcDelayAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                              ExecutorService executors, PingService pingService, FoundationService foundationService) {
        super(scheduled, instance, executors, pingService, foundationService);
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
