package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.cluster.ClusterType;

/**
 * @author chen.zhu
 * <p>
 * Aug 23, 2018
 */
public interface HealthCheckConfig {

    int delayDownAfterMilli();

    int delayDownAfterMilli(String clusterName, String fromDc, String toDc);

    int instanceLongDelayMilli();

    int pingDownAfterMilli();

    int checkIntervalMilli();

    int clusterCheckIntervalMilli();

    int getHealthyDelayMilli();

    int getHealthyDelayMilli(String clusterName, String fromDc, String toDc);

    int getRedisConfCheckIntervalMilli();

    int getSentinelCheckIntervalMilli();

    String getMinXRedisVersion();

    String getMinDiskLessReplVersion();

    boolean supportSentinelHealthCheck(ClusterType clusterType, String clusterName);

    int getNonCoreCheckIntervalMilli();
}
