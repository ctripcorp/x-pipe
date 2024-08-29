package com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class PsubAction extends AbstractHealthCheckAction<RedisHealthCheckInstance> {

    protected static final Logger logger = LoggerFactory.getLogger(PsubAction.class);

    private String[] pubSubChannelPrefix;

    public PsubAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
        this.pubSubChannelPrefix = new String[]{"xpipe*"};
    }

    @Override
    protected void doTask() {
        RedisSession session = instance.getRedisSession();
        doPSubscribe(session, new RedisSession.SubscribeCallback() {
            @Override
            public void message(String channel, String message) {
                logger.debug("[PsubAction][{}]success, channel:{}, message : {}", instance.getEndpoint(), channel, message);
                notifyListeners(new PsubActionContext(instance, message));
            }

            @Override
            public void fail(Throwable e) {
                logger.error("[PsubAction][{}] fail", instance.getEndpoint(), e);
                //ignore psub fail
            }
        }, pubSubChannelPrefix);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    protected void doPSubscribe(RedisSession session, RedisSession.SubscribeCallback callback, String... channel) {
        session.psubscribeIfAbsent(callback, channel);
    }

}
