package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.conflic;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.RedisStatsCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class ConflictCheckAction extends RedisStatsCheckAction<String, CrdtConflictStats> {

    public ConflictCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected CommandFuture<String> executeRedisCommandForStats(Callbackable<String> callback) {
        return instance.getRedisSession().crdtInfoStats(callback);
    }

    @Override
    protected ActionContext<CrdtConflictStats> generateActionContext(String result) {
        InfoResultExtractor extractor = new InfoResultExtractor(result);
        return new CrdtConflictCheckContext(instance, new CrdtConflictStats(extractor));
    }

}
