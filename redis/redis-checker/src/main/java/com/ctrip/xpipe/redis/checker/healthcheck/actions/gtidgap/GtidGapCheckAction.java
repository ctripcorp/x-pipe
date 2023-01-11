package com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class GtidGapCheckAction extends AbstractHealthCheckAction<RedisHealthCheckInstance> {

    protected static final Logger logger = LoggerFactory.getLogger(GtidGapCheckAction.class);

    protected static final int METRIC_CHECK_INTERVAL = 60 * 1000;

    public GtidGapCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected void doTask() {
        instance.getRedisSession().info("gtid", new Callbackable<String>() {
            @Override
            public void success(String message) {
                notifyListeners(new GtidGapCheckActionContext(instance, extractGapNum(message)));
            }

            @Override
            public void fail(Throwable t) {
                logger.warn("info gtid from {}:{} failed", instance.getEndpoint().getHost(), instance.getEndpoint().getPort());
            }
        });
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    int extractGapNum(String message) {
        InfoResultExtractor infoResultExtractor = new InfoResultExtractor(message);
        String gtidInfo = infoResultExtractor.extract("all");
        String[] runIds = gtidInfo.split(",");
        int gapNum = 0;
        for (String runId : runIds) {
            String[] segments = runId.split(":");
            if (segments.length > 2)
                gapNum += segments.length - 2;
        }
        return gapNum;
    }

    @Override
    protected int getBaseCheckInterval() {
        return METRIC_CHECK_INTERVAL;
    }
}
