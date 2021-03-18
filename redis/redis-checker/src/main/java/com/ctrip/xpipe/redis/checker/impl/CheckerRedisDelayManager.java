package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.RedisDelayManager;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionListener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public class CheckerRedisDelayManager implements RedisDelayManager, DelayActionListener, OneWaySupport, BiDirectionSupport {

    protected ConcurrentMap<HostPort, Long> hostPort2Delay = new ConcurrentHashMap<>();

    @Override
    public Map<HostPort, Long> getAllDelays() {
        return new HashMap<>(hostPort2Delay);
    }

    @Override
    public void onAction(DelayActionContext delayActionContext) {
        hostPort2Delay.put(delayActionContext.instance().getCheckInfo().getHostPort(),
                delayActionContext.getResult());
    }

    @Override
    public void stopWatch(HealthCheckAction<RedisHealthCheckInstance> action) {
        hostPort2Delay.remove(action.getActionInstance().getCheckInfo().getHostPort());
    }

}
