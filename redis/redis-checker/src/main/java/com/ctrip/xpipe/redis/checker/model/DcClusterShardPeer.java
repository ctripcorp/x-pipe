package com.ctrip.xpipe.redis.checker.model;

import java.io.Serializable;
import java.util.Objects;

public final class DcClusterShardPeer extends DcClusterShard implements Serializable {
    private static final String SPLITTER = ":";

    private String peerDcId;

    public DcClusterShardPeer() {

    }

    public DcClusterShardPeer(String info) {
        String[] split = info.split(SPLITTER);
        if (split.length >= 4) {
            this.dcId = split[0];
            this.clusterId = split[1];
            this.shardId = split[2];
            this.peerDcId = split[3];
        }
    }

    public DcClusterShardPeer(String dcId, String clusterId, String shardId, String peerDcId) {
        super(dcId, clusterId, shardId);
        this.peerDcId = peerDcId;
    }

    public String getPeerDcId() {
        return peerDcId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DcClusterShardPeer that = (DcClusterShardPeer) o;
        if (!super.equals(o)) return false;
        return Objects.equals(peerDcId, that.peerDcId);
    }

    public DcClusterShardPeer setPeerDcId(String peerDcId) {
        this.peerDcId = peerDcId;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDcId(), getClusterId(), getShardId(), getPeerDcId());
    }

    @Override
    public String toString() {
        return getDcId() + SPLITTER + getClusterId() + SPLITTER + getShardId() + SPLITTER + getPeerDcId();
    }

}