package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */
public class RedisMasterCheckAction extends AbstractLeaderAwareHealthCheckAction {

    public RedisMasterCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected void doTask() {
        getActionInstance().getRedisSession().role(new RedisSession.RollCallback() {
            @Override
            public void role(String role) {
                notifyListeners(new RedisMasterActionContext(instance, Server.SERVER_ROLE.of(role)));
            }

            @Override
            public void fail(Throwable e) {
                notifyListeners(new RedisMasterActionContext(instance, Server.SERVER_ROLE.UNKNOWN));
            }
        });
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }
}
