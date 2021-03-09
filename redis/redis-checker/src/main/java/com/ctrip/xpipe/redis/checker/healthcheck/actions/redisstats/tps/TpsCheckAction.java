package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tps;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.RedisStatsCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class TpsCheckAction extends RedisStatsCheckAction<String, Long> {

    public TpsCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected CommandFuture<String> executeRedisCommandForStats(Callbackable<String> callback) {
        return instance.getRedisSession().infoStats(callback);
    }

    @Override
    protected TpsActionContext generateActionContext(String result) {
        InfoResultExtractor extractor = new InfoResultExtractor(result);
        Long opsValue = extractor.extractAsLong("instantaneous_ops_per_sec");
        return new TpsActionContext(instance, null == opsValue ? 0 : opsValue);
    }

}
