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

    public DelayConfig setDcLevelHealthyDelayMilli(int dcLevelHealthyDelayMilli) {
        this.dcLevelHealthyDelayMilli = dcLevelHealthyDelayMilli;
        return this;
    }

    public int getClusterLevelHealthyDelayMilli() {
        return clusterLevelHealthyDelayMilli;
    }

    public DelayConfig setClusterLevelHealthyDelayMilli(int clusterLevelHealthyDelayMilli) {
        this.clusterLevelHealthyDelayMilli = clusterLevelHealthyDelayMilli;
        return this;
    }

    public int getDcLevelDelayDownAfterMilli() {
        return dcLevelDelayDownAfterMilli;
    }

    public DelayConfig setDcLevelDelayDownAfterMilli(int dcLevelDelayDownAfterMilli) {
        this.dcLevelDelayDownAfterMilli = dcLevelDelayDownAfterMilli;
        return this;
    }

    public int getClusterLevelDelayDownAfterMilli() {
        return clusterLevelDelayDownAfterMilli;
    }

    public DelayConfig setClusterLevelDelayDownAfterMilli(int clusterLevelDelayDownAfterMilli) {
        this.clusterLevelDelayDownAfterMilli = clusterLevelDelayDownAfterMilli;
        return this;
    }

    @Override
    public String toString() {
        return "DelayConfig{" +
                "clusterName='" + clusterName + '\'' +
                ", fromDc='" + fromDc + '\'' +
                ", toDc='" + toDc + '\'' +
                ", dcLevelHealthyDelayMilli=" + dcLevelHealthyDelayMilli +
                ", clusterLevelHealthyDelayMilli=" + clusterLevelHealthyDelayMilli +
                ", dcLevelDelayDownAfterMilli=" + dcLevelDelayDownAfterMilli +
                ", clusterLevelDelayDownAfterMilli=" + clusterLevelDelayDownAfterMilli +
                '}';
    }
}
