package com.ctrip.xpipe.redis.console.healthcheck.action.handler;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.action.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceSick;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
@Component
public class DefaultInstanceSickHandler extends AbstractHealthEventHandler<InstanceSick> implements InstanceSickHandler {

    @Autowired
    private ConsoleConfig consoleConfig;

    private static final List<HEALTH_STATE> satisfiedStates = Lists.newArrayList(HEALTH_STATE.DOWN, HEALTH_STATE.SICK, HEALTH_STATE.UNHEALTHY);

    @Override
    protected List<HEALTH_STATE> getSatisfiedStates() {
        return satisfiedStates;
    }

    @Override
    protected boolean configedNotMarkDown(AbstractInstanceEvent event) {
        RedisInstanceInfo info = event.getInstance().getRedisInstanceInfo();
        Set<Pair<String, String>> dcClusters = consoleConfig.getDelayWontMarkDownClusters();
        if(dcClusters != null && dcClusters.contains(new Pair<>(info.getDcId(), info.getClusterId()))) {
            logger.warn("[markdown] configured, not markdown, {}", info);
            alertManager.alert(info.getClusterId(), info.getShardId(), info.getHostPort(),
                    ALERT_TYPE.INSTANCE_SICK_BUT_NOT_MARK_DOWN, info.getDcId());
            return true;
        }
        return false;
    }

    @Override
    protected void doHandle(InstanceSick instanceSick) {
        tryMarkDown(instanceSick);
    }
}
