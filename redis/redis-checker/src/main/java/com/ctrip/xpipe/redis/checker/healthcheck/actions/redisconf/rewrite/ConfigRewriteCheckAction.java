package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.rewrite;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckAction;
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

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    private void checkFailReason(Throwable throwable) {
        if (throwable instanceof RedisError) {
            logger.warn("[rewrite][checkFailReason]" + throwable.getMessage(), throwable);
            String message = throwable.getMessage();
            if (message.contains("Rewriting config file:") || message.contains("The server is running without a config file")) {
                alertManager.alert(getActionInstance().getCheckInfo(), ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE,
                        throwable.getClass().getSimpleName());
                logger.error("[configRewrite] Redis:{}", instance.getCheckInfo(), throwable);
            }
        }
    }
}
