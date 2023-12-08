package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.endpoint.HostPort;

import java.io.Serializable;
import java.util.Objects;

public class DcClusterShardActive extends DcClusterShard implements Serializable {

    private boolean active;

    public DcClusterShardActive(){}

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DcClusterShardActive)) return false;
        if (!super.equals(o)) return false;
        DcClusterShardActive that = (DcClusterShardActive) o;
        return Objects.equals(isActive(), that.isActive());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isActive());
    }

    @Override
    public String toString() {
        return "DcClusterShardActive{" +
                "active=" + active +
                ", dcId='" + dcId + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", shardId='" + shardId + '\'' +
                '}';
    }
}
