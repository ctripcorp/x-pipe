package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;

/**
 * @author chen.zhu
 * <p>
 * Aug 23, 2018
 */
public interface HealthCheckConfig {

    int instanceLongDelayMilli();

    int pingDownAfterMilli();

    int checkIntervalMilli();

    int clusterCheckIntervalMilli();

    int getRedisConfCheckIntervalMilli();

    int getSentinelCheckIntervalMilli();

    String getMinXRedisVersion();

    String getMinDiskLessReplVersion();

    boolean supportSentinelHealthCheck(ClusterType clusterType, String clusterName);

    int getNonCoreCheckIntervalMilli();

    DelayConfig getDelayConfig(String clusterName, String fromDc, String toDc);
}
