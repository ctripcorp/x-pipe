package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.endpoint.HostPort;

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
                .append(" Shard: ").append(shardId)
                .append(" redis_version: ").append(redisVersion)
                .append(" xredis_version: ").append(xredisVersion);
        return sb.toString();
    }
}
