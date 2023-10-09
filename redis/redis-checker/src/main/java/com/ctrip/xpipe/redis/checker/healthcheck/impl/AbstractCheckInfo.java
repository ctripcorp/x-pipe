package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.CheckInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRule;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/14
 */
public abstract class AbstractCheckInfo implements CheckInfo {

    protected String clusterId;

    protected String activeDc;

    protected ClusterType clusterType;

    protected List<RedisCheckRule> redisCheckRules;

    protected String azGroupType;

    protected boolean asymmetricCluster;

    public AbstractCheckInfo() {

    }

    public AbstractCheckInfo(String clusterId, String activeDc, ClusterType clusterType) {
        this.clusterId = clusterId;
        this.activeDc = activeDc;
        this.clusterType = clusterType;
    }

    public AbstractCheckInfo(String clusterId, String activeDc, ClusterType clusterType, List<RedisCheckRule> redisCheckRules) {
        this(clusterId, activeDc, clusterType);
        this.redisCheckRules = redisCheckRules;
    }

    @Override
    public String getClusterId() {
        return clusterId;
    }

    @Override
    public ClusterType getClusterType() {
        return clusterType;
    }

    public String getActiveDc() {
        return activeDc;
    }

    public void setActiveDc(String activeDc) {
        this.activeDc = activeDc;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public void setClusterType(ClusterType clusterType) {
        this.clusterType = clusterType;
    }

    @Override
    public List<RedisCheckRule>  getRedisCheckRules() {
        return redisCheckRules;
    }

    public void setRedisCheckRules(List<RedisCheckRule>  redisCheckRules) {
        this.redisCheckRules = redisCheckRules;
    }

    @Override
    public String getAzGroupType() {
        return azGroupType;
    }

    @Override
    public void setAzGroupType(String azGroupType) {
        this.azGroupType = azGroupType;
    }

    @Override
    public void setAsymmetricCluster(boolean asymmetricCluster) {
        this.asymmetricCluster = asymmetricCluster;
    }

    @Override
    public boolean isAsymmetricCluster() {
        return asymmetricCluster;
    }

}
