package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.RedisStatsCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class CrdtInfoStatsAction extends RedisStatsCheckAction<String, InfoResultExtractor> {
    
    public CrdtInfoStatsAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected CommandFuture<String> executeRedisCommandForStats(Callbackable<String> callback) {
        return getActionInstance().getRedisSession().crdtInfoStats(callback);
    }

    @Override
    protected ActionContext<InfoResultExtractor, RedisHealthCheckInstance> generateActionContext(String result) {
        InfoResultExtractor extractor = new InfoResultExtractor(result);
        return new CrdtInfoStatsContext(instance, extractor);
    }
}

