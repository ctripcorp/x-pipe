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
        this.dcName = sentinelTbl.getDcInfo() == null ? null : sentinelTbl.getDcInfo().getDcName();
    }

    public long getSentinelId() {
        return sentinelId;
    }

    public SentinelInstanceModel setSentinelId(long sentinelId) {
        this.sentinelId = sentinelId;
        return this;
    }

    public long getSentinelGroupId() {
        return sentinelGroupId;
    }

    public SentinelInstanceModel setSentinelGroupId(long sentinelGroupId) {
        this.sentinelGroupId = sentinelGroupId;
        return this;
    }

    public long getDcId() {
        return dcId;
    }

    public SentinelInstanceModel setDcId(long dcId) {
        this.dcId = dcId;
        return this;
    }

    public String getSentinelIp() {
        return sentinelIp;
    }

    public SentinelInstanceModel setSentinelIp(String sentinelIp) {
        this.sentinelIp = sentinelIp;
        return this;
    }

    public int getSentinelPort() {
        return sentinelPort;
    }

    public SentinelInstanceModel setSentinelPort(int sentinelPort) {
        this.sentinelPort = sentinelPort;
        return this;
    }

    public String getDcName() {
        return dcName;
    }

    public SentinelInstanceModel setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    @Override
    public String toString() {
        return "SentinelInstanceModel{" +
                "sentinelId=" + sentinelId +
                ", sentinelGroupId=" + sentinelGroupId +
                ", dcId=" + dcId +
                ", sentinelIp='" + sentinelIp + '\'' +
                ", sentinelPort=" + sentinelPort +
                '}';
    }
}
