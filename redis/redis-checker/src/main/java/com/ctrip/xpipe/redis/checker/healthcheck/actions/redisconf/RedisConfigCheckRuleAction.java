package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class RedisConfigCheckRuleAction extends AbstractRedisConfigRuleAction {
    private static final Logger logger = LoggerFactory.getLogger(RedisConfigCheckRuleAction.class);

    public RedisConfigCheckRuleAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors, List<RedisCheckRule> redisCheckRules ) {
        super(scheduled, instance, executors, redisCheckRules);
    }

    @Override
    protected void doTask() {
        redisCheckRules.forEach(redisCheckRule -> {
            instance.getRedisSession().ConfigGet(new Callbackable<String>() {
                @Override
                public void success(String message) {
                    if(!redisCheckRule.getParams().get(EXPECTED_VAULE).equals(message)) {
                        notifyListeners(new RedisConfigCheckRuleActionContext(instance, message, redisCheckRule));
                   }
                }

                @Override
                public void fail(Throwable throwable) {
                    logger.error("[RedisConfigCheckRuleAction] redis: {}, {} name :{}", getActionInstance().getCheckInfo(), redisCheckRule.getCheckType(), redisCheckRule.getParams().get(CONFIG_CHECK_NAME), throwable);
                }
            }, redisCheckRule.getParams().get(CONFIG_CHECK_NAME));
        });
    }


    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }
}
