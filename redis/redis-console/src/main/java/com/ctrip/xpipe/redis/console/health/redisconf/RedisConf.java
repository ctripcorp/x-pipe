package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author chen.zhu
 * <p>
 * Sep 28, 2017
 */
public class RedisConf {

    HostPort hostPort;

    private String clusterId;

    private String shardId;

    private String redisVersion;

    private String xredisVersion;

    public RedisConf(HostPort hostPort, String clusterId, String shardId) {
        this.hostPort = hostPort;
        this.clusterId = clusterId;
        this.shardId = shardId;
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    public void setHostPort(HostPort hostPort) {
        this.hostPort = hostPort;
    }

    public String getRedisVersion() {
        return redisVersion;
    }

    public void setRedisVersion(String redisVersion) {
        this.redisVersion = redisVersion;
    }

    public String getXredisVersion() {
        return xredisVersion;
    }

    public void setXredisVersion(String xredisVersion) {
        this.xredisVersion = xredisVersion;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Cluster: ").append(clusterId)
                .append(", Shard: ").append(shardId).append(", ")
                .append(hostPort.toString())
                .append(", redis_version: ").append(redisVersion)
                .append(", xredis_version: ").append(xredisVersion);
        return sb.toString();
    }


    @Override
    public int hashCode() {
        return hostPort.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof RedisConf)) {
            return false;
        }
        if(obj == this) {
            return true;
        }
        RedisConf redisConf = (RedisConf) obj;
        return ObjectUtils.equals(redisConf.clusterId, this.clusterId)
                && ObjectUtils.equals(redisConf.shardId, this.shardId)
                && ObjectUtils.equals(redisConf.hostPort, this.hostPort);
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }
}
