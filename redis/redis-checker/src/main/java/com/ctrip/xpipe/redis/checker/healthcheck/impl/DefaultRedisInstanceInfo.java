package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DefaultRedisInstanceInfo extends AbstractCheckInfo implements RedisInstanceInfo {

    private String dcId;

    private String shardId;

    private HostPort hostPort;

    private boolean isMaster;

    private boolean crossRegion;

    public DefaultRedisInstanceInfo(String dcId, String clusterId, String shardId, HostPort hostPort, String activeDc, ClusterType clusterType) {
        super(clusterId, activeDc, clusterType);
        this.dcId = dcId;
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.hostPort = hostPort;
        this.activeDc = activeDc;
        this.clusterType = clusterType;
    }

    @Override
    public ClusterShardHostPort getClusterShardHostport() {
        return new ClusterShardHostPort(clusterId, shardId, hostPort);
    }

    @Override
    public String getShardId() {
        return shardId;
    }

    @Override
    public String getDcId() {
        return dcId;
    }

    @Override
    public HostPort getHostPort() {
        return hostPort;
    }

    @Override
    public boolean isMaster() {
        return isMaster;
    }

    public void isMaster(boolean master) {
        isMaster = master;
    }

    @Override
    public boolean isInActiveDc() {
        if (null == activeDc) return false;
        return this.dcId.equalsIgnoreCase(activeDc);
    }

    @Override
    public boolean isCrossRegion() {
        return crossRegion;
    }

    @Override
    public String toString() {
        return StringUtil.join(", ", dcId, clusterId, shardId, hostPort, crossRegion ? "proxied" : "normal");
    }

    public DefaultRedisInstanceInfo setCrossRegion(boolean crossRegion) {
        this.crossRegion = crossRegion;
        return this;
    }
}
