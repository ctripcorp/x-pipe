package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 2:59 PM
 */
public class InfoAction extends AbstractHealthCheckAction<RedisHealthCheckInstance> {

    private static final Logger logger = LoggerFactory.getLogger(InfoAction.class);

    public InfoAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected void doTask() {
        CommandFuture<String> info = instance.getRedisSession().info("", new Callbackable<String>() {
            @Override
            public void success(String message) {
                notifyListeners(new RawInfoActionContext(instance, message));
            }

            @Override
            public void fail(Throwable t) {
                notifyListeners(new RawInfoActionContext(instance, t));
            }
        });
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }
}
