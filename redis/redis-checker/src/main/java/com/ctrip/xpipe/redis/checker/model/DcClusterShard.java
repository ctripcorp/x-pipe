package com.ctrip.xpipe.redis.checker.model;

import java.io.Serializable;
import java.util.Objects;

public class DcClusterShard implements Serializable {

    protected static final String SPLITTER = ":";

    protected String dcId;

    protected String clusterId;

    protected String shardId;

    public DcClusterShard() {

    }

    public DcClusterShard(String dcId, String clusterId, String shardId) {
        this.dcId = dcId;
        this.clusterId = clusterId;
        this.shardId = shardId;
    }

    public DcClusterShard(String info) {
        String[] split = info.split(SPLITTER);
        if (split.length >= 3) {
            this.dcId = split[0];
            this.clusterId = split[1];
            this.shardId = split[2];
        }
    }

    public DcClusterShard setDcId(String dcId) {
        this.dcId = dcId;
        return this;
    }

    public DcClusterShard setClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public DcClusterShard setShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }

    public String getDcId() {
        return dcId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getShardId() {
        return shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DcClusterShard that = (DcClusterShard) o;
        return Objects.equals(dcId, that.dcId) &&
                Objects.equals(clusterId, that.clusterId) &&
                Objects.equals(shardId, that.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dcId, clusterId, shardId);
    }

    @Override
    public String toString() {
        return getDcId() + SPLITTER + getClusterId() + SPLITTER + getShardId();
    }
}
