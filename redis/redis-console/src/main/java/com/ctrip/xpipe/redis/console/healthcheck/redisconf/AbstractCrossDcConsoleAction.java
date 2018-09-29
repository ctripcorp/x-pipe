package com.ctrip.xpipe.redis.console.healthcheck.redisconf;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Sep 28, 2018
 */
public abstract class AbstractCrossDcConsoleAction extends AbstractHealthCheckAction {

    private CrossDcClusterServer server;

    public AbstractCrossDcConsoleAction(ScheduledExecutorService scheduled,
                                        RedisHealthCheckInstance instance,
                                        ExecutorService executors, CrossDcClusterServer server) {
        super(scheduled, instance, executors);
        this.server = server;
    }

    @Override
    public void doStart() {
        if(server.amILeader()) {
           super.doStart();
        }
    }

}
