package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public abstract class AbstractKeeperInfoCommand<T extends AbstractActionContext> extends KeeperStatsCheckAction<String, InfoResultExtractor> {

    public AbstractKeeperInfoCommand(ScheduledExecutorService scheduled, KeeperHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected T generateActionContext(String result) {
        return createActionContext(result);
    }

    protected abstract T createActionContext(String extractor);

}
