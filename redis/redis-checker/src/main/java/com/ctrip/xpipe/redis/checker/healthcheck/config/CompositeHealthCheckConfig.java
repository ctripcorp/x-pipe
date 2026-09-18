package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.RelationsService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import com.google.common.base.Strings;
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

    public CompositeHealthCheckConfig(RedisInstanceInfo instanceInfo, CheckerConfig checkerConfig, RelationsService relationsService, boolean isCrossRegion) {
        logger.info("[CompositeHealthCheckConfig] {}", instanceInfo);
        if(isCrossRegion) {
            config = new ProxyEnabledHealthCheckConfig(checkerConfig, relationsService);
            logger.info("[CompositeHealthCheckConfig][proxied] ping down time: {}", config.pingDownAfterMilli());
        } else {
            config = new DefaultHealthCheckConfig(checkerConfig, relationsService);
        }
        logger.info("[CompositeHealthCheckConfig][{}] [config: {}]", instanceInfo, config.getClass().getSimpleName());
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
    public boolean supportSentinelBeacon(long clusterOrgId, String clusterName) {
        return config.supportSentinelBeacon(clusterOrgId, clusterName);
    }

    @Override
    public boolean supportCollectInfo(ClusterType clusterType) {
        return config.supportCollectInfo(clusterType);
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

    @Override
    public DelayConfig getDelayConfig(String clusterName, String fromDc, String toDc) {
        return config.getDelayConfig(clusterName, fromDc, toDc);
    }

    @Override
    public boolean isReachable(String srcDc, String dstDc) {
        if (Strings.isNullOrEmpty(srcDc) || Strings.isNullOrEmpty(dstDc))
            return false;

        return config.isReachable(srcDc, dstDc);
    }
}
