package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.backstreaming;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.RedisStatsCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author lishanglin
 * date 2021/1/26
 */
public class BackStreamingAction extends RedisStatsCheckAction<String, Boolean> {

    public BackStreamingAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors) {
        super(scheduled, instance, executors);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected CommandFuture<String> executeRedisCommandForStats(Callbackable<String> callback) {
        return instance.getRedisSession().crdtInfoReplication(callback);
    }

    @Override
    protected ActionContext<Boolean, RedisHealthCheckInstance> generateActionContext(String result) {
        InfoResultExtractor extractor = new InfoResultExtractor(result);
        String backStreamingStats = extractor.extract("backstreaming");
        if (!StringUtil.isEmpty(backStreamingStats) && backStreamingStats.trim().equals("1")) {
            return new BackStreamingContext(instance, true);
        } else {
            return new BackStreamingContext(instance, false);
        }
    }

}
