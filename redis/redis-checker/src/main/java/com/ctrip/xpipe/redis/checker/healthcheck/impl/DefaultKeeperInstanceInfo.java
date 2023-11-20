package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRule;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class DefaultKeeperInstanceInfo extends AbstractCheckInfo implements KeeperInstanceInfo {

    private String dcId;

    private String shardId;

    private HostPort hostPort;

    private boolean isActive;

    public DefaultKeeperInstanceInfo() {
        super();
    }


    public DefaultKeeperInstanceInfo(String dcId, String clusterId, String shardId, HostPort hostPort, String activeDc, ClusterType clusterType) {
        this(dcId, clusterId, shardId, hostPort, activeDc, clusterType, null);
    }

    public DefaultKeeperInstanceInfo(String dcId, String clusterId, String shardId, HostPort hostPort, String activeDc, ClusterType clusterType, List<RedisCheckRule> redisCheckRules) {
        super(clusterId, activeDc, clusterType, redisCheckRules);
        this.dcId = dcId;
        this.shardId = shardId;
        this.hostPort = hostPort;
    }

    public DefaultKeeperInstanceInfo setDcId(String dcId) {
        this.dcId = dcId;
        return this;
    }

    public DefaultKeeperInstanceInfo setShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }

    public DefaultKeeperInstanceInfo setHostPort(HostPort hostPort) {
        this.hostPort = hostPort;
        return this;
    }

    public DefaultKeeperInstanceInfo setActive(boolean active) {
        isActive = active;
        return this;
    }

    @Override
    public String toString() {
        return "DefaultKeeperInstanceInfo{" +
                "dcId='" + dcId + '\'' +
                ", shardId='" + shardId + '\'' +
                ", hostPort=" + hostPort +
                ", isMaster=" + isActive +
                ", clusterId='" + clusterId + '\'' +
                ", activeDc='" + activeDc + '\'' +
                ", clusterType=" + clusterType +
                '}';
    }

    @JsonIgnore
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
    public boolean isActive() {
        return isActive;
    }

    @Override
    public HostPort getHostPort() {
        return hostPort;
    }
}
