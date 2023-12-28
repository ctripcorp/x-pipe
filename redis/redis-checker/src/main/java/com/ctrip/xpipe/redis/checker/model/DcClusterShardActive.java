package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.endpoint.HostPort;

import java.io.Serializable;
import java.util.Objects;

public class DcClusterShardActive extends DcClusterShard implements Serializable {

    private boolean active;

    private int port;

    public DcClusterShardActive(){}

    public DcClusterShardActive(String dcId, String clusterId, String shardId, boolean active, int port) {
        super(dcId, clusterId, shardId);
        this.active = active;
        this.port = port;
    }

    public DcClusterShardActive(String dcId, String clusterId, String shardId, boolean active) {
        super(dcId, clusterId, shardId);
        this.active = active;
    }

    public DcClusterShardActive(DcClusterShard dcClusterShard, boolean active) {
        super(dcClusterShard.getDcId(), dcClusterShard.getClusterId(), dcClusterShard.getShardId());
        this.active = active;
    }

    public DcClusterShardActive(String info) {
        String[] split = info.split(SPLITTER);
        if (split.length >= 5) {
            this.dcId = split[0];
            this.clusterId = split[1];
            this.shardId = split[2];
            this.active = Boolean.parseBoolean(split[3]);
            this.port = Integer.parseInt(split[4]);
        }
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
        return new StringBuilder().append(getDcId()).append(SPLITTER).append(getClusterId()).append(SPLITTER).append(getShardId()).append(SPLITTER).append(isActive()).append(SPLITTER).append(getPort()).toString();
    }

}
