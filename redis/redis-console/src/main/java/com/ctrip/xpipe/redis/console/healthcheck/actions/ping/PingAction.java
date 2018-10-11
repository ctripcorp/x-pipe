package com.ctrip.xpipe.redis.console.healthcheck.actions.ping;

import com.ctrip.xpipe.redis.console.healthcheck.session.PingCallback;
import com.ctrip.xpipe.redis.console.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class PingAction extends AbstractHealthCheckAction<PingActionContext> {

    public PingAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    public void doTask() {
        instance.getRedisSession().ping(new PingCallback() {
            @Override
            public void pong(String pongMsg) {
                notifyListeners(new PingActionContext(instance, PingCommand.PONG.equalsIgnoreCase(pongMsg)));
            }

            @Override
            public void fail(Throwable th) {
                notifyListeners(new PingActionContext(instance, false));
            }
        });
    }

}
