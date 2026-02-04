package com.ctrip.xpipe.redis.core.entity;

import java.util.Objects;

/**
 * @author ayq
 * <p>
 * 2022/4/1 23:26
 */
public class ApplierTransMeta {

    private String clusterName;

    private Long clusterDbId;

    private Long shardDbId;

    private ApplierMeta applierMeta;

    private Long qpsThreshold;

    private Long bytesPerSecondThreshold;

    private Long memoryThreshold;

    private Long concurrencyThreshold;

    private Integer stateThreadNum = 1;

    private Integer workerThreadNum = 1;

    private String subenv;

    private int batchSize;

    //for json conversion
    public ApplierTransMeta() {}

    public ApplierTransMeta(String clusterName, Long clusterDbId, Long shardDbId, ApplierMeta applierMeta) {
        this.clusterName = clusterName;
        this.clusterDbId = clusterDbId;
        this.shardDbId = shardDbId;
        this.applierMeta = applierMeta;
    }

    public ApplierTransMeta(Long clusterDbId, Long shardDbId, ApplierMeta applierMeta) {
        this(null, clusterDbId, shardDbId, applierMeta);
    }

    public ApplierMeta getApplierMeta() {
        return applierMeta;
    }

    public ApplierTransMeta setApplierMeta(ApplierMeta applierMeta) {
        this.applierMeta = applierMeta;
        return this;
    }

    public Long getClusterDbId() {
        return clusterDbId;
    }

    public ApplierTransMeta setClusterDbId(Long clusterDbId) {
        this.clusterDbId = clusterDbId;
        return this;
    }

    public Long getShardDbId() {
        return shardDbId;
    }

    public ApplierTransMeta setShardDbId(Long shardDbId) {
        this.shardDbId = shardDbId;
        return this;
    }

    public String getClusterName() {
        return clusterName;
    }

    public ApplierTransMeta setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public Long getQpsThreshold() {
        return qpsThreshold;
    }

    public ApplierTransMeta setQpsThreshold(Long qpsThreshold) {
        this.qpsThreshold = qpsThreshold;
        return this;
    }

    public Long getBytesPerSecondThreshold() {
        return bytesPerSecondThreshold;
    }

    public ApplierTransMeta setBytesPerSecondThreshold(Long bytesPerSecondThreshold) {
        this.bytesPerSecondThreshold = bytesPerSecondThreshold;
        return this;
    }

    public Long getMemoryThreshold() {
        return memoryThreshold;
    }

    public ApplierTransMeta setMemoryThreshold(Long memoryThreshold) {
        this.memoryThreshold = memoryThreshold;
        return this;
    }

    public Long getConcurrencyThreshold() {
        return concurrencyThreshold;
    }

    public ApplierTransMeta setConcurrencyThreshold(Long concurrencyThreshold) {
        this.concurrencyThreshold = concurrencyThreshold;
        return this;
    }

    public Integer getStateThreadNum() {
        return stateThreadNum;
    }

    public ApplierTransMeta setStateThreadNum(Integer stateThreadNum) {
        this.stateThreadNum = stateThreadNum;
        return this;
    }

    public Integer getWorkerThreadNum() {
        return workerThreadNum;
    }

    public ApplierTransMeta setWorkerThreadNum(Integer workerThreadNum) {
        this.workerThreadNum = workerThreadNum;
        return this;
    }

    public String getSubenv() {
        return subenv;
    }

    public ApplierTransMeta setSubenv(String subenv) {
        this.subenv = subenv;
        return this;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public ApplierTransMeta setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
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
