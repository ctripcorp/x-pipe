package com.ctrip.xpipe.redis.checker.model;

public class DcRelation {
    private String dcs;
    private int distance;

    public DcRelation setDcs(String dcs) {
        this.dcs = dcs;
        return this;
    }
    public String getDcs() {
        return dcs;
    }

    public DcRelation setDistance(int distance) {
        this.distance = distance;
        return this;
    }
    public int getDistance() {
        return distance;
    }
}
