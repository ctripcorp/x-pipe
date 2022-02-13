package com.ctrip.xpipe.redis.console.model;

import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class SentinelGroupModel {

    private long sentinelGroupId;
    private List<SentinelInstanceModel> sentinels = new ArrayList<>();
    private String sentinelType;
    private int shardCount;

    public SentinelGroupModel() {}

    public SentinelGroupModel(SentinelGroupTbl sentinelGroupTbl) {
        this.sentinelGroupId = sentinelGroupTbl.getSentinelGroupId();
        this.sentinelType = sentinelGroupTbl.getClusterType();
    }

    public long getSentinelGroupId() {
        return sentinelGroupId;
    }

    public void setSentinelGroupId(long sentinelGroupId) {
        this.sentinelGroupId = sentinelGroupId;
    }

    public List<SentinelInstanceModel> getSentinels() {
        return sentinels;
    }

    public SentinelGroupModel addSentinel(SentinelInstanceModel sentinel) {
        this.sentinels.add(sentinel);
        return this;
    }

    public String getSentinelType() {
        return sentinelType;
    }

    public void setSentinelType(String sentinelType) {
        this.sentinelType = sentinelType;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    public String getSentinelsAddressString() {
        Set<String> sentinelAddressStrings = new HashSet<>();
        sentinels.forEach(sentinelInstanceModel -> {
            sentinelAddressStrings.add(String.format("%s:%d", sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort()));
        });
        return StringUtils.join(sentinelAddressStrings, ",");
    }

    public Set<Long> dcIds(){
       return sentinels.stream().map(sentinelInstanceModel -> sentinelInstanceModel.getDcId()).collect(Collectors.toSet());
    }

    public Map<String, Long> dcInfos() {
        Map<String, Long> dcInfos = new HashMap<>();
        sentinels.forEach(sentinelInstanceModel -> {
            dcInfos.put(sentinelInstanceModel.getDcName().toUpperCase(), sentinelInstanceModel.getDcId());
        });
        return dcInfos;
    }

}
