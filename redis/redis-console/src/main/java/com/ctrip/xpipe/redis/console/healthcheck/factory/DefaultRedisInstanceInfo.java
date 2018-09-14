package com.ctrip.xpipe.redis.console.healthcheck.factory;

import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DefaultRedisInstanceInfo implements RedisInstanceInfo {

    private String dcId;

    private String clusterId;

    private String shardId;

    private HostPort hostPort;

    private boolean isMaster;

    public DefaultRedisInstanceInfo(String dcId, String clusterId, String shardId, HostPort hostPort) {
        this.dcId = dcId;
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.hostPort = hostPort;
    }

    @Override
    public ClusterShardHostPort getClusterShardHostport() {
        return new ClusterShardHostPort(clusterId, shardId, hostPort);
    }

    @Override
    public String getClusterId() {
        return clusterId;
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
    public String toString() {
        return StringUtil.join(", ", dcId, clusterId, shardId, hostPort);
    }
}
