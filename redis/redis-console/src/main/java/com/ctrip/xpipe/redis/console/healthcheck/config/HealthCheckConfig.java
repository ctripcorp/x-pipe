package com.ctrip.xpipe.redis.console.healthcheck.config;

/**
 * @author chen.zhu
 * <p>
 * Aug 23, 2018
 */
public interface HealthCheckConfig {

    int delayDownAfterMilli();

    int pingDownAfterMilli();

    int checkIntervalMilli();

    int getHealthyDelayMilli();

    int getRedisConfCheckIntervalMilli();

    int getSentinelCheckIntervalMilli();

    String getMinXRedisVersion();

    String getMinDiskLessReplVersion();

}
