package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.CheckInfo;

/**
 * @author lishanglin
 * date 2021/1/14
 */
public abstract class AbstractCheckInfo implements CheckInfo {

    protected String clusterId;

    protected String activeDc;

    protected ClusterType clusterType;

    public AbstractCheckInfo() {

    }

    public AbstractCheckInfo(String clusterId, String activeDc, ClusterType clusterType) {
        this.clusterId = clusterId;
        this.activeDc = activeDc;
        this.clusterType = clusterType;
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


}
