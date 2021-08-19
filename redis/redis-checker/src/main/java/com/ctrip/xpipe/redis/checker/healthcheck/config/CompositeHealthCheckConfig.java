package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

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
    public int getRedisConfCheckIntervalMilli() {
        return config.getRedisConfCheckIntervalMilli();
    }

    @Override
    public int getSentinelCheckIntervalMilli() {
        return config.getSentinelCheckIntervalMilli();
    }

    @Override
    public boolean checkClusterType() {
        return config.checkClusterType();
    }

    @Override
    public Set<String> commonClustersSupportSentinelCheck() {
        return config.commonClustersSupportSentinelCheck();
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
