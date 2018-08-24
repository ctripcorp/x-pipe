package com.ctrip.xpipe.redis.console.healthcheck.ping;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
@Service
public class DefaultPingService implements PingService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPingService.class);

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Override
    public boolean isRedisAlive(HostPort hostPort) {
        try {
            RedisHealthCheckInstance instance = instanceManager.findRedisHealthCheckInstance(hostPort);
            return instance.getHealthCheckContext().getPingContext().getPingStatus().isHealthy();
        } catch (Exception e) {
            logger.error("[isRedisAlive]", e);
        }
        return false;
    }
}
