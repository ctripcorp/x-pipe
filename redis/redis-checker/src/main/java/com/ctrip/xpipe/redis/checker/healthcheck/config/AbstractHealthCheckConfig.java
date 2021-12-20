package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;

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
    public int instanceLongDelayMilli() {
        return checkerConfig.getInstanceLongDelayMilli();
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
    public int getHealthyLeastNotifyIntervalMilli() {
        return checkerConfig.getHealthyLeastNotifyIntervalMilli();
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
    public String getMinXRedisVersion() {
        return checkerConfig.getXRedisMinimumRequestVersion();
    }

    @Override
    public String getMinDiskLessReplVersion() {
        return checkerConfig.getReplDisklessMinRedisVersion();
    }

    @Override
    public boolean supportSentinelHealthCheck(ClusterType clusterType, String clusterName) {
        return checkerConfig.supportSentinelHealthCheck(clusterType, clusterName);
    }
}
