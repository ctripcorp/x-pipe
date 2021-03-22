package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public class DefaultPingService implements PingService, PingActionListener, OneWaySupport, BiDirectionSupport {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPingService.class);

    @Autowired
    private CheckerConfig config;

    private ConcurrentMap<HostPort, Long> hostPort2LastPong = new ConcurrentHashMap<>();

    @Override
    public void updateRedisAlives(Map<HostPort, Boolean> redisAlives) {
        throw new UnsupportedOperationException("updateRedisAlives not support");
    }

    @Override
    public boolean isRedisAlive(HostPort hostPort) {
        Long lastPongTime = hostPort2LastPong.get(hostPort);
        return isRedisAlive(lastPongTime);
    }

    @Override
    public Map<HostPort, Boolean> getAllRedisAlives() {
        Map<HostPort, Boolean> redisAlives = new HashMap<>();
        hostPort2LastPong.forEach(((hostPort, lastPongTime) -> redisAlives.put(hostPort, isRedisAlive(lastPongTime))));
        return redisAlives;
    }

    private boolean isRedisAlive(Long lastPongTime) {
        long maxNoPongTime = 2 * config.getRedisReplicationHealthCheckInterval();
        return lastPongTime != null && System.currentTimeMillis() - lastPongTime < maxNoPongTime;
    }

    @Override
    public void onAction(PingActionContext pingActionContext) {
        if(pingActionContext.getResult()) {
            hostPort2LastPong.put(pingActionContext.instance().getCheckInfo().getHostPort(), System.currentTimeMillis());
        }
    }

    @Override
    public void stopWatch(HealthCheckAction<RedisHealthCheckInstance> action) {
        hostPort2LastPong.remove(action.getActionInstance().getCheckInfo().getHostPort());
    }
}
