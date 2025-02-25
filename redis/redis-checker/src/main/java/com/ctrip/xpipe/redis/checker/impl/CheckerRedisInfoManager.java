package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.RedisInfoManager;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.RawInfoActionContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 4:46 PM
 */
public class CheckerRedisInfoManager implements RedisInfoManager, InfoActionListener, OneWaySupport, BiDirectionSupport {

    protected ConcurrentMap<HostPort, InfoActionContext> hostPort2Info = new ConcurrentHashMap<>();

    @Override
    public InfoActionContext getInfoByHostPort(HostPort hostPort) {
        return hostPort2Info.get(hostPort);
    }

    @Override
    public Map<HostPort, InfoActionContext> getAllInfos() {
        return hostPort2Info;
    }
    @Override
    public void onAction(RawInfoActionContext rawInfoActionContext) {
        hostPort2Info.put(rawInfoActionContext.instance().getCheckInfo().getHostPort(), ()->rawInfoActionContext);
    }

    @Override
    public void stopWatch(HealthCheckAction<RedisHealthCheckInstance> action) {
        hostPort2Info.remove(action.getActionInstance().getCheckInfo().getHostPort());
    }
}
