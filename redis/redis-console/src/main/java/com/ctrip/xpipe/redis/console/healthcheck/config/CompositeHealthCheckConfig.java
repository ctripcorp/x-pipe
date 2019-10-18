package com.ctrip.xpipe.redis.console.healthcheck.config;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public class CompositeHealthCheckConfig implements HealthCheckConfig {

    private HealthCheckConfig config;

    public CompositeHealthCheckConfig(RedisInstanceInfo instanceInfo, ConsoleConfig consoleConfig) {
        if(instanceInfo.isReplThroughProxy()) {
            config = new ProxyEnabledHealthCheckConfig(consoleConfig);
        } else {
            config = new DefaultHealthCheckConfig(consoleConfig);
        }
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
