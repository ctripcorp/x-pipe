package com.ctrip.xpipe.redis.console.model;

import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class SentinelGroupModel {

    private long sentinelGroupId;
    private List<SentinelInstanceModel> sentinels = new ArrayList<>();
    private String clusterType;
    private String desc = "";
    private int shardCount;
    private int active = 1;

    public SentinelGroupModel() {}

    public SentinelGroupModel(SentinelGroupTbl sentinelGroupTbl) {
        this.sentinelGroupId = sentinelGroupTbl.getSentinelGroupId();
        this.clusterType = sentinelGroupTbl.getClusterType();
        this.active = sentinelGroupTbl.getActive();
    }

    public long getSentinelGroupId() {
        return sentinelGroupId;
    }

    public SentinelGroupModel setSentinelGroupId(long sentinelGroupId) {
        this.sentinelGroupId = sentinelGroupId;
        return this;
    }

    public List<SentinelInstanceModel> getSentinels() {
        return sentinels;
    }

    public SentinelGroupModel addSentinel(SentinelInstanceModel sentinel) {
        this.sentinels.add(sentinel);
        return this;
    }

    public SentinelGroupModel setSentinels(List<SentinelInstanceModel> sentinels) {
        this.sentinels = sentinels;
        return this;
    }

    public String getClusterType() {
        return clusterType;
    }

    public SentinelGroupModel setClusterType(String clusterType) {
        this.clusterType = clusterType;
        return this;
    }

    public int getShardCount() {
        return shardCount;
    }

    public SentinelGroupModel setShardCount(int shardCount) {
        this.shardCount = shardCount;
        return this;
    }

    public String getSentinelsAddressString() {
        Set<String> sentinelAddressStrings = new HashSet<>();
        sentinels.forEach(sentinelInstanceModel -> {
            sentinelAddressStrings.add(String.format("%s:%d", sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort()));
        });
        return StringUtils.join(sentinelAddressStrings, ",");
    }

    public Set<Long> dcIds(){
       return sentinels.stream().map(SentinelInstanceModel::getDcId).collect(Collectors.toSet());
    }

    public Map<String, Long> dcInfos() {
        Map<String, Long> dcInfos = new HashMap<>();
        sentinels.forEach(sentinelInstanceModel -> {
            dcInfos.put(sentinelInstanceModel.getDcName(), sentinelInstanceModel.getDcId());
        });
        return dcInfos;
    }

    public boolean isActive() {
        return active == 1;
    }

    public SentinelGroupModel setActive(int active) {
        this.active = active;
        return this;
    }

    public int getActive() {
        return active;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return "SentinelGroupModel{" +
                "sentinelGroupId=" + sentinelGroupId +
                ", sentinels=" + sentinels +
                ", clusterType='" + clusterType + '\'' +
                ", shardCount=" + shardCount +
                ", active=" + active+
                '}';
    }
}
