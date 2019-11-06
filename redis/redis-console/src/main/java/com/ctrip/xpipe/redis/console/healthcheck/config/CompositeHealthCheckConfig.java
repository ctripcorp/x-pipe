package com.ctrip.xpipe.redis.console.healthcheck.config;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public class CompositeHealthCheckConfig implements HealthCheckConfig {

    private Logger logger = LoggerFactory.getLogger(CompositeHealthCheckConfig.class);

    private HealthCheckConfig config;

    public CompositeHealthCheckConfig(RedisInstanceInfo instanceInfo, ConsoleConfig consoleConfig) {
        logger.info("[CompositeHealthCheckConfig] {}", instanceInfo);
        if(instanceInfo.isReplThroughProxy()) {
            config = new ProxyEnabledHealthCheckConfig(consoleConfig);
            logger.info("[CompositeHealthCheckConfig][proxied] ping down time: {}", config.pingDownAfterMilli());
        } else {
            config = new DefaultHealthCheckConfig(consoleConfig);
        }
        logger.info("[CompositeHealthCheckConfig][{}] [config: {}]", instanceInfo, config.getClass().getSimpleName());
    }

    @Override
    public int delayDownAfterMilli() {
        return config.delayDownAfterMilli();
    }

    @Override
    public int pingDownAfterMilli() {
        return config.pingDownAfterMilli();
    }

    @Override
    public int checkIntervalMilli() {
        return config.checkIntervalMilli();
    }

    @Override
    public int getHealthyDelayMilli() {
        return config.getHealthyDelayMilli();
    }

    @Override
    public int getRedisConfCheckIntervalMilli() {
        return config.getRedisConfCheckIntervalMilli();
    }

    @Override
    public String getMinXRedisVersion() {
        return config.getMinXRedisVersion();
    }

    @Override
    public String getMinDiskLessReplVersion() {
        return config.getMinDiskLessReplVersion();
    }
}
