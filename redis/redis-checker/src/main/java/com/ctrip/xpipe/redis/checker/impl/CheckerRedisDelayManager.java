package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.RedisDelayManager;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.AbstractDelayActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.HeteroDelayActionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public class CheckerRedisDelayManager extends AbstractDelayActionListener implements RedisDelayManager, DelayActionListener, OneWaySupport, BiDirectionSupport {

    protected ConcurrentMap<HostPort, Long> hostPort2Delay = new ConcurrentHashMap<>();
    protected Map<Long, Long> heteroShardsDelay = new ConcurrentHashMap<>();

    @Override
    public Map<HostPort, Long> getAllDelays() {
        return new HashMap<>(hostPort2Delay);
    }

    @Override
    public Map<Long, Long> getAllHeteroShardsDelays() {
        return new HashMap<>(heteroShardsDelay);
    }

    @Override
    public void onAction(DelayActionContext delayActionContext) {
        if (delayActionContext instanceof HeteroDelayActionContext)
            heteroShardsDelay.put(((HeteroDelayActionContext) delayActionContext).getShardDbId(), delayActionContext.getResult());
        else
            hostPort2Delay.put(delayActionContext.instance().getCheckInfo().getHostPort(),
                    delayActionContext.getResult());
    }

    @Override
    public boolean supportInstance(RedisHealthCheckInstance instance) {
        return true;
    }

    @Override
    public void stopWatch(HealthCheckAction<RedisHealthCheckInstance> action) {
        RedisInstanceInfo instanceInfo = action.getActionInstance().getCheckInfo();
        hostPort2Delay.remove(instanceInfo.getHostPort());
    }

}
