package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.AbstractRedisConfigRuleAction.CONFIG_CHECK_NAME;
import static com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.AbstractRedisConfigRuleAction.EXPECTED_VAULE;

public class RedisConfigCheckRuleActionListener implements HealthCheckActionListener<RedisConfigCheckRuleActionContext, HealthCheckAction<RedisHealthCheckInstance>>, OneWaySupport, BiDirectionSupport {

    private AlertManager alertManager;

    public RedisConfigCheckRuleActionListener(AlertManager alertManager) {
        this.alertManager = alertManager;
    }

    @Override
    public void onAction(RedisConfigCheckRuleActionContext redisConfigCheckRuleActionContext) {

        String alertMessage = String.format("%s:%s should be %s, but was %s",
                redisConfigCheckRuleActionContext.getRedisCheckRule().getCheckType(),
                redisConfigCheckRuleActionContext.getRedisCheckRule().getParams().get(CONFIG_CHECK_NAME),
                redisConfigCheckRuleActionContext.getRedisCheckRule().getParams().get(EXPECTED_VAULE),
                redisConfigCheckRuleActionContext.getResult());

        alertManager.alert(redisConfigCheckRuleActionContext.instance().getCheckInfo(), ALERT_TYPE.REDIS_CONIFIG_CHECK_FAIL, alertMessage);
    }

    @Override
    public boolean worksfor(ActionContext t) {
        return t instanceof RedisConfigCheckRuleActionContext;
    }

    @Override
    public void stopWatch(HealthCheckAction<RedisHealthCheckInstance> action) {

    }
}
