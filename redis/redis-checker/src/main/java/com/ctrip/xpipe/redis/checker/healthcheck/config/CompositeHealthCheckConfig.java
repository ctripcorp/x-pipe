package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
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

    public CompositeHealthCheckConfig(RedisInstanceInfo instanceInfo, CheckerConfig checkerConfig) {
        logger.info("[CompositeHealthCheckConfig] {}", instanceInfo);
        if(instanceInfo.isCrossRegion()) {
            config = new ProxyEnabledHealthCheckConfig(checkerConfig);
            logger.info("[CompositeHealthCheckConfig][proxied] ping down time: {}", config.pingDownAfterMilli());
        } else {
            config = new DefaultHealthCheckConfig(checkerConfig);
        }
        logger.info("[CompositeHealthCheckConfig][{}] [config: {}]", instanceInfo, config.getClass().getSimpleName());
    }

    @Override
    public int delayDownAfterMilli() {
        return config.delayDownAfterMilli();
    }

    @Override
    public int instanceLongDelayMilli() {
        return config.instanceLongDelayMilli();
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
    public int clusterCheckIntervalMilli() {
        return config.clusterCheckIntervalMilli();
    }

    @Override
    public int getHealthyDelayMilli() {
        return config.getHealthyDelayMilli();
    }

    @Override
    public int getHealthyLeastNotifyIntervalMilli() {
        return config.getHealthyLeastNotifyIntervalMilli();
    }

    @Override
    public int getRedisConfCheckIntervalMilli() {
        return config.getRedisConfCheckIntervalMilli();
    }

    @Override
    public int getSentinelCheckIntervalMilli() {
        return config.getSentinelCheckIntervalMilli();
    }

    @Override
    public boolean supportSentinelHealthCheck(ClusterType clusterType, String clusterName) {
        return config.supportSentinelHealthCheck(clusterType, clusterName);
    }

    @Override
    public int getNonCoreCheckIntervalMilli() {
        return config.getNonCoreCheckIntervalMilli();
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
