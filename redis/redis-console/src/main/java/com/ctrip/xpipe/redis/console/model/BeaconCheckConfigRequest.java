package com.ctrip.xpipe.redis.console.model;

import java.util.List;

public class BeaconCheckConfigRequest {

    private String clusterName;

    private String dc;

    private List<String> shards;

    public String getClusterName() {
        return clusterName;
    }

    public BeaconCheckConfigRequest setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getDc() {
        return dc;
    }

    public BeaconCheckConfigRequest setDc(String dc) {
        this.dc = dc;
        return this;
    }

    public List<String> getShards() {
        return shards;
    }

    public BeaconCheckConfigRequest setShards(List<String> shards) {
        this.shards = shards;
        return this;
    }
}
