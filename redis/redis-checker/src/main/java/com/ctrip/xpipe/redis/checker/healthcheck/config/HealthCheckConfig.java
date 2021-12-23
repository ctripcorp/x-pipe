package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.cluster.ClusterType;

/**
 * @author chen.zhu
 * <p>
 * Aug 23, 2018
 */
public interface HealthCheckConfig {

    int delayDownAfterMilli();

    int instanceLongDelayMilli();

    int pingDownAfterMilli();

    int checkIntervalMilli();

    int clusterCheckIntervalMilli();

    int getHealthyDelayMilli();

    int getHealthyLeastNotifyIntervalMilli();

    int getRedisConfCheckIntervalMilli();

    int getSentinelCheckIntervalMilli();

    String getMinXRedisVersion();

    String getMinDiskLessReplVersion();

    boolean supportSentinelHealthCheck(ClusterType clusterType, String clusterName);
}
