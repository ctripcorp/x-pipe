package com.ctrip.xpipe.redis.checker.healthcheck.actions.crdtredisconf;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.AbstractRedisConfigRuleAction;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRule;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRuleAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class CRDTRedisConfigCheckRuleAction extends AbstractRedisConfigRuleAction{
    private static final Logger logger = LoggerFactory.getLogger(CRDTRedisConfigCheckRuleAction.class);

    public CRDTRedisConfigCheckRuleAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors, AlertManager alertManager, List<RedisConfigCheckRule> redisConfigCheckRules) {
        super(scheduled, instance, executors, alertManager, redisConfigCheckRules);
    }

    @Override
    protected void doTask() {
        redisConfigCheckRules.forEach(redisConfigCheckRule -> {
            instance.getRedisSession().CRDTConfigGet(new Callbackable<String>() {
                @Override
                public void success(String message) {
                    if(!redisConfigCheckRule.getExpectedVaule().equals(message)) {
                        String alertMessage = String.format("crdt config:%s of redis:%s should be %s", redisConfigCheckRule.getConfigCheckName(), getActionInstance().getEndpoint().toString(), redisConfigCheckRule.getExpectedVaule());
                        logger.warn("{}", alertMessage);
                        alertManager.alert(getActionInstance().getCheckInfo(), ALERT_TYPE.REDIS_CONIFIG_CHECK_FAIL, alertMessage);
                    }
                }

                @Override
                public void fail(Throwable throwable) {
                    logger.error("[CRDTRedisConfigCheckRuleAction] redis: {}, config name :{}", getActionInstance().getCheckInfo(), redisConfigCheckRule.getConfigCheckName(), throwable);
                }
            }, redisConfigCheckRule.getConfigCheckName());
        });
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }
}
