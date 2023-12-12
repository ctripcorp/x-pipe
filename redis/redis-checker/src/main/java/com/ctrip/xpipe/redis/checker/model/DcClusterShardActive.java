package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.endpoint.HostPort;

import java.io.Serializable;
import java.util.Objects;

public class DcClusterShardActive extends DcClusterShard implements Serializable {

    private boolean active;

    private int port;

    public DcClusterShardActive(){}

    public DcClusterShardActive(String dcId, String clusterId, String shardId, Boolean active, int port) {
        super(dcId, clusterId, shardId);
        this.active = active;
        this.port = port;
    }

    public DcClusterShardActive(String dcId, String clusterId, String shardId, Boolean active) {
        super(dcId, clusterId, shardId);
        this.active = active;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DcClusterShardActive)) return false;
        if (!super.equals(o)) return false;
        DcClusterShardActive that = (DcClusterShardActive) o;
        return isActive() == that.isActive();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isActive());
    }

    @Override
    public String toString() {
        return "DcClusterShardActive{" +
                "active=" + active +
                ", port=" + port +
                ", dcId='" + dcId + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", shardId='" + shardId + '\'' +
                '}';
    }

}
