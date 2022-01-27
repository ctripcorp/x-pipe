package com.ctrip.xpipe.redis.console.model;

import java.util.List;
import java.util.Map;

public class SentinelGroupInfo {

    private long sentinelGroupId;
    private Map<String, List<SentinelInfo>> sentinels;
    private String appliedClusterTypes;
    private int shardCount;

    public SentinelGroupInfo(long sentinelGroupId, String appliedClusterTypes) {
        this.sentinelGroupId = sentinelGroupId;
        this.appliedClusterTypes = appliedClusterTypes;
    }

    public long getSentinelGroupId() {
        return sentinelGroupId;
    }

    public void setSentinelGroupId(long sentinelGroupId) {
        this.sentinelGroupId = sentinelGroupId;
    }

    public Map<String, List<SentinelInfo>> getSentinels() {
        return sentinels;
    }

    public void setSentinels(Map<String, List<SentinelInfo>> sentinels) {
        this.sentinels = sentinels;
    }

    public String getAppliedClusterTypes() {
        return appliedClusterTypes;
    }

    public void setAppliedClusterTypes(String appliedClusterTypes) {
        this.appliedClusterTypes = appliedClusterTypes;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }
}
