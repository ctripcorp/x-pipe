package com.ctrip.xpipe.redis.checker.healthcheck.actions.crdtredisconf;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.AbstractRedisConfigRuleAction;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRule;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRuleActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class CRDTRedisConfigCheckRuleAction extends AbstractRedisConfigRuleAction{
    private static final Logger logger = LoggerFactory.getLogger(CRDTRedisConfigCheckRuleAction.class);

    public CRDTRedisConfigCheckRuleAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors, List<RedisCheckRule> redisCheckRules) {
        super(scheduled, instance, executors, redisCheckRules);
    }

    @Override
    protected void doTask() {
        redisCheckRules.forEach(redisCheckRule -> {
            instance.getRedisSession().CRDTConfigGet(new Callbackable<String>() {
                @Override
                public void success(String message) {
                    if(!redisCheckRule.getParams().get(EXPECTED_VAULE).equals(message)) {
                        notifyListeners(new RedisConfigCheckRuleActionContext(instance, message, redisCheckRule));
                    }
                }

                @Override
                public void fail(Throwable throwable) {
                    logger.error("[CRDTRedisConfigCheckRuleAction] redis: {}, config name :{}", getActionInstance().getCheckInfo(), redisCheckRule.getParams().get(CONFIG_CHECK_NAME), throwable);
                }
            }, redisCheckRule.getParams().get(CONFIG_CHECK_NAME));
        });
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }
}
