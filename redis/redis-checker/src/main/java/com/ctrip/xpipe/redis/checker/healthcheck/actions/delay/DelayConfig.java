package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

public class DelayConfig {

    private String clusterName;
    private String fromDc;
    private String toDc;

    private int dcLevelHealthyDelayMilli;
    private int clusterLevelHealthyDelayMilli;

    private int dcLevelDelayDownAfterMilli;
    private int clusterLevelDelayDownAfterMilli;


    public DelayConfig(String clusterName, String fromDc, String toDc) {
        this.clusterName = clusterName;
        this.fromDc = fromDc;
        this.toDc = toDc;
    }

    public int getDcLevelHealthyDelayMilli() {
        return dcLevelHealthyDelayMilli;
    }

    public void setDcLevelHealthyDelayMilli(int dcLevelHealthyDelayMilli) {
        this.dcLevelHealthyDelayMilli = dcLevelHealthyDelayMilli;
    }

    public int getClusterLevelHealthyDelayMilli() {
        return clusterLevelHealthyDelayMilli;
    }

    public void setClusterLevelHealthyDelayMilli(int clusterLevelHealthyDelayMilli) {
        this.clusterLevelHealthyDelayMilli = clusterLevelHealthyDelayMilli;
    }

    public int getDcLevelDelayDownAfterMilli() {
        return dcLevelDelayDownAfterMilli;
    }

    public void setDcLevelDelayDownAfterMilli(int dcLevelDelayDownAfterMilli) {
        this.dcLevelDelayDownAfterMilli = dcLevelDelayDownAfterMilli;
    }

    public int getClusterLevelDelayDownAfterMilli() {
        return clusterLevelDelayDownAfterMilli;
    }

    public void setClusterLevelDelayDownAfterMilli(int clusterLevelDelayDownAfterMilli) {
        this.clusterLevelDelayDownAfterMilli = clusterLevelDelayDownAfterMilli;
    }
}
