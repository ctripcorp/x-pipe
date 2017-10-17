package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author chen.zhu
 * <p>
 * Oct 12, 2017
 */
public class RedisAlert {
    private HostPort hostPort;
    private String clusterId;
    private String shardId;
    private String message;
    private ALERT_TYPE alertType;

    public RedisAlert(HostPort hostPort, String clusterId, String shardId, String message, ALERT_TYPE alertType) {
        this.hostPort = hostPort;
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.message = message;
        this.alertType = alertType;
    }

    @Override
    public int hashCode() {
        int hashCode = alertType.hashCode();
        if(clusterId != null) {
            hashCode = hashCode * 31 + clusterId.hashCode();
        }
        if(shardId != null) {
            hashCode = hashCode * 31 + shardId.hashCode();
        }
        if(hostPort != null) {
            hashCode = hashCode * 31 + hostPort.hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof RedisAlert)) return false;
        if(obj == this) return true;
        RedisAlert other = (RedisAlert) obj;
        return ObjectUtils.equals(other.getAlertType(), this.alertType) &&
                ObjectUtils.equals(other.getClusterId(), this.clusterId) &&
                ObjectUtils.equals(other.getShardId(), this.shardId) &&
                ObjectUtils.equals(other.getHostPort(), this.hostPort);
    }

    @Override
    public String toString() {
        return "Alert: " + alertType + ", Cluster: " + clusterId
                + ", Shard: " + shardId + ", HostPort: " + hostPort + ", Message: " + message;
    }

    public ALERT_TYPE getAlertType() {
        return alertType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    public void setHostPort(HostPort hostPort) {
        this.hostPort = hostPort;
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

    public static class RedisAlertBuilder {
        private HostPort hostPort;
        private String clusterId;
        private String shardId;
        private String message;
        private ALERT_TYPE alertType;

        public RedisAlertBuilder hostPort(HostPort hostPort) {
            this.hostPort = hostPort;
            return this;
        }

        public RedisAlertBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public RedisAlertBuilder shardId(String shardId) {
            this.shardId = shardId;
            return this;
        }

        public RedisAlertBuilder message(String message) {
            this.message = message;
            return this;
        }

        public RedisAlertBuilder alertType(ALERT_TYPE alertType) {
            this.alertType = alertType;
            return this;
        }

        public RedisAlert createRedisAlert() {
            return new RedisAlert(this.hostPort, this.clusterId, this.shardId, this.message, this.alertType);
        }
    }
}
