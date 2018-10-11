package com.ctrip.xpipe.redis.console.healthcheck.actions.ping;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
@Service
public class DefaultPingService implements PingService, PingActionListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPingService.class);

    @Autowired
    private ConsoleConfig config;

    private ConcurrentMap<HostPort, Long> hostPort2LastPong = new ConcurrentHashMap<>();

    @Override
    public boolean isRedisAlive(HostPort hostPort) {
        Long lastPongTime = hostPort2LastPong.get(hostPort);
        long maxNoPongTime = 2 * config.getRedisReplicationHealthCheckInterval();
        return lastPongTime != null && System.currentTimeMillis() - lastPongTime < maxNoPongTime;
    }

    @Override
    public void onAction(PingActionContext pingActionContext) {
        if(pingActionContext.getResult()) {
            hostPort2LastPong.put(pingActionContext.instance().getRedisInstanceInfo().getHostPort(), System.currentTimeMillis());
        }
    }

    @Override
    public boolean worksfor(ActionContext t) {
        return t instanceof PingActionContext;
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        hostPort2LastPong.remove(action.getActionInstance().getRedisInstanceInfo().getHostPort());
    }
}
