package com.ctrip.xpipe.redis.checker.model;

public class Relation {
    private String src;
    private String dst;
    private int distance;
    private boolean biDirection = true;

    public String getSrc() {
        return src;
    }

    public Relation setSrc(String src) {
        this.src = src;
        return this;
    }

    public String getDst() {
        return dst;
    }

    public Relation setDst(String dst) {
        this.dst = dst;
        return this;
    }

    public int getDistance() {
        return distance;
    }

    public Relation setDistance(int distance) {
        this.distance = distance;
        return this;
    }

    public boolean isBiDirection() {
        return biDirection;
    }

    public Relation setBiDirection(boolean biDirection) {
        this.biDirection = biDirection;
        return this;
    }
}
