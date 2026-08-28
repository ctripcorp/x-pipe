package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;
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
    protected boolean shouldCheck(HealthCheckInstance instance) {
        // bypass the supportHealthCheck gate so info can be collected for
        // SINGLE_DC/LOCAL_DC clusters (whose supportHealthCheck is false);
        // only the controllers (e.g. CurrentDcInfoController) decide.
        return super.shouldCheckInstance(instance);
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
