package com.ctrip.xpipe.redis.console.model;

public class SentinelInfo {

    private long sentinelId;

    private long sentinelGroupId;

    private long dc_id;

    private String sentinelIp;

    private int sentinelPort;

    public SentinelInfo(){}

    public SentinelInfo(long sentinelId, long sentinelGroupId, long dc_id, String sentinelIp, int sentinelPort) {
        this.sentinelId = sentinelId;
        this.sentinelGroupId = sentinelGroupId;
        this.dc_id = dc_id;
        this.sentinelIp = sentinelIp;
        this.sentinelPort = sentinelPort;
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

    public long getDc_id() {
        return dc_id;
    }

    public void setDc_id(long dc_id) {
        this.dc_id = dc_id;
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
}
