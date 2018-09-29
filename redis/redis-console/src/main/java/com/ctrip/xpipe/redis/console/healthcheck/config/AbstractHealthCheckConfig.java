package com.ctrip.xpipe.redis.console.healthcheck.config;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;

/**
 * @author chen.zhu
 * <p>
 * Aug 29, 2018
 */
public abstract class AbstractHealthCheckConfig implements HealthCheckConfig {

    protected ConsoleConfig consoleConfig;

    public AbstractHealthCheckConfig(ConsoleConfig consoleConfig) {
        this.consoleConfig = consoleConfig;
    }

    @Override
    public int delayDownAfterMilli() {
        return consoleConfig.getDownAfterCheckNums() * consoleConfig.getHealthyDelayMilli();
    }

    @Override
    public int pingDownAfterMilli() {
        return consoleConfig.getPingDownAfterMilli();
    }

    @Override
    public int checkIntervalMilli() {
        return consoleConfig.getRedisReplicationHealthCheckInterval();
    }

    @Override
    public int getHealthyDelayMilli() {
        return consoleConfig.getHealthyDelayMilli();
    }

    @Override
    public int getRedisConfCheckIntervalMilli() {
        return consoleConfig.getRedisConfCheckIntervalMilli();
    }

    @Override
    public String getMinXRedisVersion() {
        return consoleConfig.getXRedisMinimumRequestVersion();
    }

    @Override
    public String getMinDiskLessReplVersion() {
        return consoleConfig.getReplDisklessMinRedisVersion();
    }
}
