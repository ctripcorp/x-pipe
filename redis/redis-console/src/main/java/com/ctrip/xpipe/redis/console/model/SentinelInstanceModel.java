package com.ctrip.xpipe.redis.console.model;

public class SentinelInstanceModel {

    private long sentinelId;

    private long sentinelGroupId;

    private long dcId;

    private String sentinelIp;

    private int sentinelPort;

    private String dcName;

    public SentinelInstanceModel(){}

    public SentinelInstanceModel(SentinelTbl sentinelTbl) {
        this.sentinelId = sentinelTbl.getSentinelId();
        this.sentinelGroupId = sentinelTbl.getSentinelGroupId();
        this.dcId = sentinelTbl.getDcId();
        this.sentinelIp = sentinelTbl.getSentinelIp();
        this.sentinelPort = sentinelTbl.getSentinelPort();
        this.dcName=sentinelTbl.getDcName();
    }

    public long getSentinelId() {
        return sentinelId;
    }

    public void setSentinelId(long sentinelId) {
        this.sentinelId = sentinelId;
    }

    public long getSentinelGroupId() {
        return sentinelGroupId;
    }

    public void setSentinelGroupId(long sentinelGroupId) {
        this.sentinelGroupId = sentinelGroupId;
    }

    public long getDcId() {
        return dcId;
    }

    public void setDcId(long dcId) {
        this.dcId = dcId;
    }

    public String getSentinelIp() {
        return sentinelIp;
    }

    public void setSentinelIp(String sentinelIp) {
        this.sentinelIp = sentinelIp;
    }

    public int getSentinelPort() {
        return sentinelPort;
    }

    public void setSentinelPort(int sentinelPort) {
        this.sentinelPort = sentinelPort;
    }

    public String getDcName() {
        return dcName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }
}
