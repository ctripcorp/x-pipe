package com.ctrip.xpipe.redis.console.healthcheck.redis;

import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;

import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public class DefaultRedisInstanceInfo implements RedisInstanceInfo {

    private String dcId;

    private String clusterId;

    private String shardId;

    private HostPort hostPort;

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
    public String toString() {
        return String.format("dc: %s, cluster: %s, shard: %s, redis: %s", dcId, clusterId, shardId, hostPort);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultRedisInstanceInfo that = (DefaultRedisInstanceInfo) o;
        return Objects.equals(dcId, that.dcId) &&
                Objects.equals(clusterId, that.clusterId) &&
                Objects.equals(shardId, that.shardId) &&
                Objects.equals(hostPort, that.hostPort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dcId, clusterId, shardId, hostPort);
    }
}
