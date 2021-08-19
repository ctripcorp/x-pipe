package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.redis.checker.config.CheckerConfig;

import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Aug 29, 2018
 */
public abstract class AbstractHealthCheckConfig implements HealthCheckConfig {

    protected CheckerConfig checkerConfig;

    public AbstractHealthCheckConfig(CheckerConfig checkerConfig) {
        this.checkerConfig = checkerConfig;
    }

    @Override
    public int delayDownAfterMilli() {
        return checkerConfig.getDownAfterCheckNums() * checkerConfig.getHealthyDelayMilli();
    }

    @Override
    public int pingDownAfterMilli() {
        return checkerConfig.getPingDownAfterMilli();
    }

    @Override
    public int checkIntervalMilli() {
        return checkerConfig.getRedisReplicationHealthCheckInterval();
    }

    @Override
    public int clusterCheckIntervalMilli() {
        return checkerConfig.getClusterHealthCheckInterval();
    }

    @Override
    public int getHealthyDelayMilli() {
        return checkerConfig.getHealthyDelayMilli();
    }

    @Override
    public int getRedisConfCheckIntervalMilli() {
        return checkerConfig.getRedisConfCheckIntervalMilli();
    }

    @Override
    public int getSentinelCheckIntervalMilli() {
        return checkerConfig.getSentinelCheckIntervalMilli();
    }

    @Override
    public boolean checkClusterType() {
        return checkerConfig.checkClusterType();
    }

    @Override
    public Set<String> commonClustersSupportSentinelCheck() {
        return checkerConfig.commonClustersSupportSentinelCheck();
    }

    @Override
    public String getMinXRedisVersion() {
        return checkerConfig.getXRedisMinimumRequestVersion();
    }

    @Override
    public String getMinDiskLessReplVersion() {
        return checkerConfig.getReplDisklessMinRedisVersion();
    }
}
