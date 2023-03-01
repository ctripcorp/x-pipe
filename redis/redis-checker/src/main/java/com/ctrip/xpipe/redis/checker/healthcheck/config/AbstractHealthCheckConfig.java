package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;

/**
 * @author chen.zhu
 * <p>
 * Aug 29, 2018
 */
public abstract class AbstractHealthCheckConfig implements HealthCheckConfig {

    protected CheckerConfig checkerConfig;

    protected DcRelationsService dcRelationsService;

    public AbstractHealthCheckConfig(CheckerConfig checkerConfig, DcRelationsService dcRelationsService) {
        this.checkerConfig = checkerConfig;
        this.dcRelationsService = dcRelationsService;
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
    public int downAfterCheckNums() {
        return checkerConfig.getDownAfterCheckNums();
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

    @Override
    public int getNonCoreCheckIntervalMilli(){
        return checkerConfig.getNonCoreCheckIntervalMilli();
    }

    @Override
    public DelayConfig getDelayConfig(String clusterName, String fromDc, String toDc) {
        DelayConfig config = new DelayConfig(clusterName, fromDc, toDc);
        config.setDcLevelHealthyDelayMilli(getHealthyDelayMilli());
        config.setDcLevelDelayDownAfterMilli(downAfterCheckNums() * getHealthyDelayMilli());
        config.setClusterLevelHealthyDelayMilli(getHealthyDelayMilli());
        config.setClusterLevelDelayDownAfterMilli(downAfterCheckNums() * getHealthyDelayMilli());

        Integer dcsDelay = dcRelationsService.getDcsDelay(fromDc, toDc);
        if (dcsDelay != null) {
            config.setDcLevelHealthyDelayMilli(dcsDelay);
            config.setDcLevelDelayDownAfterMilli(downAfterCheckNums() * dcsDelay);
        }

        Integer clusterDcsDelay = dcRelationsService.getClusterDcsDelay(clusterName, fromDc, toDc);
        if (clusterDcsDelay != null) {
            config.setClusterLevelHealthyDelayMilli(clusterDcsDelay);
            config.setClusterLevelDelayDownAfterMilli(downAfterCheckNums() * clusterDcsDelay);
        }

        return config;
    }

}
