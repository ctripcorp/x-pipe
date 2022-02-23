package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.CheckInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRule;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/14
 */
public abstract class AbstractCheckInfo implements CheckInfo {

    protected String clusterId;

    protected String activeDc;

    protected ClusterType clusterType;

    protected List<RedisConfigCheckRule> redisConfigCheckRules;

    public AbstractCheckInfo() {

    }

    public AbstractCheckInfo(String clusterId, String activeDc, ClusterType clusterType) {
        this.clusterId = clusterId;
        this.activeDc = activeDc;
        this.clusterType = clusterType;
    }

    public AbstractCheckInfo(String clusterId, String activeDc, ClusterType clusterType, List<RedisConfigCheckRule>  redisConfigCheckRules) {
        this(clusterId, activeDc, clusterType);
        this.redisConfigCheckRules = redisConfigCheckRules;
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
    public List<RedisConfigCheckRule>  getRedisConfigCheckRules() {
        return redisConfigCheckRules;
    }

    public void setRedisConfigCheckRules(List<RedisConfigCheckRule>  redisConfigCheckRules) {
        this.redisConfigCheckRules = redisConfigCheckRules;
    }
}
