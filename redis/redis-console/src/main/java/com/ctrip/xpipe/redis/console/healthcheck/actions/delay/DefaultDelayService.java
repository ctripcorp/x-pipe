package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
@Component
public class DefaultDelayService implements DelayService, DelayActionListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDelayService.class);

    private ConcurrentMap<HostPort, Long> hostPort2Delay = Maps.newConcurrentMap();

    @Override
    public long getDelay(HostPort hostPort) {
        long result = hostPort2Delay.getOrDefault(hostPort, DelayAction.SAMPLE_LOST_AND_NO_PONG);
        return TimeUnit.NANOSECONDS.toMillis(result);
    }

    @Override
    public void onAction(DelayActionContext delayActionContext) {
        hostPort2Delay.put(delayActionContext.instance().getRedisInstanceInfo().getHostPort(),
                delayActionContext.getResult());
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        hostPort2Delay.remove(action.getActionInstance().getRedisInstanceInfo().getHostPort());
    }
}
