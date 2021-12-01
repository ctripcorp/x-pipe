package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoCommandAction;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.RedisStatsCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class CrdtInfoReplicationAction extends AbstractInfoCommandAction<CrdtInfoReplicationContext> {

    public CrdtInfoReplicationAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected CrdtInfoReplicationContext createActionContext(String extractor) {
        return new CrdtInfoReplicationContext(instance, extractor);
    }

    @Override
    protected CommandFuture<String> executeRedisCommandForStats(Callbackable<String> callback) {
        return getActionInstance().getRedisSession().crdtInfoReplication(callback);
    }

    
}
