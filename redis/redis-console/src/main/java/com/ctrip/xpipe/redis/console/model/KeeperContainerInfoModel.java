package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.codec.JsonCodec;

import java.io.Serializable;

public class KeeperContainerInfoModel implements Serializable {
    private static final long serialVersionUID = 1L;

    private long id;
    private boolean active;
    /** 容器化后 ip 可能随重建变化，与 hostname、port 分开暴露，不再拼成 address */
    private String ip;
    private int port;
    private String dcName;
    private String orgName;
    private String tag;
    private String azName;
    private long keeperCount;
    private long shardCount;
    private long clusterCount;
    private String diskType;
    private long logicalBuId;
    private String keepercontainerHostName;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

    public long getLogicalBuId() {
        return logicalBuId;
    }

    public void setLogicalBuId(long logicalBuId) {
        this.logicalBuId = logicalBuId;
    }

    public String getKeepercontainerHostName() {
        return keepercontainerHostName;
    }

    public void setKeepercontainerHostName(String keepercontainerHostName) {
        this.keepercontainerHostName = keepercontainerHostName;
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
