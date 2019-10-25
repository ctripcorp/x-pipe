package com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.rewrite;

import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.RedisConfigCheckAction;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;

/**
 * @author chen.zhu
 * <p>
 * Oct 07, 2018
 */
public class ConfigRewriteCheckAction extends RedisConfigCheckAction {

    private static final Logger logger = LoggerFactory.getLogger(ConfigRewriteCheckAction.class);

    public ConfigRewriteCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                                    ExecutorService executors, AlertManager alertManager) {
        super(scheduled, instance, executors, alertManager);
    }

    @Override
    protected void doTask() {
        instance.getRedisSession().configRewrite(new BiConsumer<String, Throwable>() {
            @Override
            public void accept(String s, Throwable throwable) {
                if(throwable != null) {
                    checkFailReason(throwable);
                } else {
                    checkPassed();
                }
            }
        });
    }

    private void checkFailReason(Throwable throwable) {
        if(throwable instanceof CommandTimeoutException) {
            return;
        }
        if(throwable instanceof RedisError) {
            if(throwable.getMessage().contains("during loading")) {
                return;
            }
        }
        alertManager.alert(getActionInstance().getRedisInstanceInfo(), ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE,
                throwable.getClass().getSimpleName());
        logger.error("[configRewrite] Redis:{}", instance.getRedisInstanceInfo(), throwable);
    }
}
