package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class CRDTDelayAction extends DelayAction {

    public CRDTDelayAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                           ExecutorService executors, PingService pingService, FoundationService foundationService) {
        super(scheduled, instance, executors, pingService, foundationService);
    }

    @Override
    protected void doSubscribe(RedisSession session, SubscribeCallback callback, String... channel) {
        session.crdtsubscribeIfAbsent(callback, channel);
    }

    @Override
    protected void doPublish(RedisSession session, String channel, String message) {
        session.crdtpublish(channel, message);
    }

}
