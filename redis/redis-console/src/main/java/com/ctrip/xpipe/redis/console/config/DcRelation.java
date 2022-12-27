package com.ctrip.xpipe.redis.console.config;

public class DcRelation {
    private String dcs;
    private int distance;

    public String getDcs() {
        return dcs;
    }

    public DcRelation setDcs(String dcs) {
        this.dcs = dcs;
        return this;
    }

    public int getDistance() {
        return distance;
    }

    public DcRelation setDistance(int distance) {
        this.distance = distance;
        return this;
    }
}
