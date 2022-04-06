package com.ctrip.xpipe.redis.core.entity;

import java.util.Objects;

/**
 * @author ayq
 * <p>
 * 2022/4/1 23:26
 */
public class ApplierTransMeta {

    private Long clusterDbId;

    private Long shardDbId;

    private ApplierMeta applierMeta;

    //for json conversion
    public ApplierTransMeta() {}

    public ApplierTransMeta(Long clusterDbId, Long shardDbId, ApplierMeta applierMeta) {
        this.clusterDbId = clusterDbId;
        this.shardDbId = shardDbId;
        this.applierMeta = applierMeta;
    }

    public ApplierMeta getApplierMeta() {
        return applierMeta;
    }

    public void setApplierMeta(ApplierMeta applierMeta) {
        this.applierMeta = applierMeta;
    }

    public Long getClusterDbId() {
        return clusterDbId;
    }

    public void setClusterDbId(Long clusterDbId) {
        this.clusterDbId = clusterDbId;
    }

    public Long getShardDbId() {
        return shardDbId;
    }

    public void setShardDbId(Long shardDbId) {
        this.shardDbId = shardDbId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplierTransMeta that = (ApplierTransMeta) o;
        return Objects.equals(clusterDbId, that.clusterDbId) &&
                Objects.equals(shardDbId, that.shardDbId) &&
                Objects.equals(applierMeta, that.applierMeta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterDbId, shardDbId, applierMeta);
    }

    @Override
    public String toString() {
        return String.format("[%d,%d-%s:%d]", clusterDbId, shardDbId, applierMeta.getIp(), applierMeta.getPort());
    }
}
