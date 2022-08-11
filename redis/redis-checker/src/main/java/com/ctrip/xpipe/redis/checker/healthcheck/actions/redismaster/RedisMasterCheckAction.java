package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */
public class RedisMasterCheckAction extends AbstractLeaderAwareHealthCheckAction<RedisHealthCheckInstance> {

    public RedisMasterCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected boolean shouldCheck(HealthCheckInstance instance) {
        return super.shouldCheckInstance(instance);
    }

    @Override
    protected void doTask() {
        getActionInstance().getRedisSession().role(new RedisSession.RollCallback() {
            @Override
            public void role(String role, Role detail) {
                notifyListeners(new RedisMasterActionContext(instance, detail));
            }

            @Override
            public void fail(Throwable th) {
                notifyListeners(new RedisMasterActionContext(instance, th));
            }
        });
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected int getCheckInitialDelay(int baseInterval) {
        return Math.abs(random.nextInt(baseInterval) % baseInterval);
    }

}
