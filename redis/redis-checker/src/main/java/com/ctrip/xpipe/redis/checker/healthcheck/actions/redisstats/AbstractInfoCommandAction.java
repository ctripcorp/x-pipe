package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public abstract class AbstractInfoCommandAction<T extends AbstractActionContext> extends RedisStatsCheckAction<String, InfoResultExtractor>{

    public AbstractInfoCommandAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected T generateActionContext(String result) {
        InfoResultExtractor extractor = new InfoResultExtractor(result);
        return createActionContext(extractor);
    }

    protected abstract T createActionContext(InfoResultExtractor extractor);
}
