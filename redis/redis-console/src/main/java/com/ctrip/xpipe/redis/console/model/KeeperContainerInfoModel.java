package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.HostPort;

import java.io.Serializable;

public class KeeperContainerInfoModel implements Serializable {
    private static final long serialVersionUID = 1L;

    private long id;
    private boolean active;
    private HostPort addr;
    private String dcName;
    private String orgName;
    private String tag;
    private String azName;
    private long keeperCount;
    private long shardCount;
    private long clusterCount;
    private String diskType;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public HostPort getAddr() {
        return addr;
    }

    public void setAddr(HostPort addr) {
        this.addr = addr;
    }

    public long getKeeperCount() {
        return keeperCount;
    }

    public void setKeeperCount(long keeperCount) {
        this.keeperCount = keeperCount;
    }

    public long getShardCount() {
        return shardCount;
    }

    public void setShardCount(long shardCount) {
        this.shardCount = shardCount;
    }

    public long getClusterCount() {
        return clusterCount;
    }

    public void setClusterCount(long clusterCount) {
        this.clusterCount = clusterCount;
    }

    public String getDcName() {
        return dcName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }

    public String getOrgName() {
        return orgName;
    }

    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getAzName() {
        return azName;
    }

    public void setAzName(String azName) {
        this.azName = azName;
    }

    public boolean isActive() {
        return active;
    }

    public KeeperContainerInfoModel setActive(boolean active) {
        this.active = active;
        return this;
    }

    public String getDiskType() {
        return diskType;
    }

    public void setDiskType(String diskType) {
        this.diskType = diskType;
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
