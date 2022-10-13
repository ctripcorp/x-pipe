package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.HostPort;

import java.io.Serializable;

public class AppliercontainerInfoModel implements Serializable {

    private static final long serialVersionUID = 1L;

    private long id;

    private HostPort addr;

    private String dcName;

    private String orgName;

    private String azName;

    private boolean active;

    private long applierCount;

    private long shardCount;

    private long clusterCount;

    public AppliercontainerInfoModel() {
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public long getId() {
        return id;
    }

    public AppliercontainerInfoModel setId(long id) {
        this.id = id;
        return this;
    }

    public HostPort getAddr() {
        return addr;
    }

    public AppliercontainerInfoModel setAddr(HostPort addr) {
        this.addr = addr;
        return this;
    }

    public String getDcName() {
        return dcName;
    }

    public AppliercontainerInfoModel setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public String getOrgName() {
        return orgName;
    }

    public AppliercontainerInfoModel setOrgName(String orgName) {
        this.orgName = orgName;
        return this;
    }

    public String getAzName() {
        return azName;
    }

    public AppliercontainerInfoModel setAzName(String azName) {
        this.azName = azName;
        return this;
    }

    public long getApplierCount() {
        return applierCount;
    }

    public AppliercontainerInfoModel setApplierCount(long applierCount) {
        this.applierCount = applierCount;
        return this;
    }

    public long getShardCount() {
        return shardCount;
    }

    public AppliercontainerInfoModel setShardCount(long shardCount) {
        this.shardCount = shardCount;
        return this;
    }

    public long getClusterCount() {
        return clusterCount;
    }

    public AppliercontainerInfoModel setClusterCount(long clusterCount) {
        this.clusterCount = clusterCount;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public AppliercontainerInfoModel setActive(boolean active) {
        this.active = active;
        return this;
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
