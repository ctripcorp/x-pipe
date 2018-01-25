package com.ctrip.xpipe.redis.console.model;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Jan 22, 2018
 */
public class SentinelUsageModel {

    private String dcName;

    private Map<String, Long> sentinelUsages;

    public SentinelUsageModel(String dcName) {
        this.dcName = dcName;
        sentinelUsages = Maps.newHashMap();
    }

    public SentinelUsageModel(String dcName, int n) {
        this.dcName = dcName;
        this.sentinelUsages = Maps.newHashMapWithExpectedSize(n);
    }

    public String getDcName() {
        return dcName;
    }

    public SentinelUsageModel setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public SentinelUsageModel addSentinelUsage(String sentinelAddress, long usage) {
        this.sentinelUsages.put(sentinelAddress, usage);
        return this;
    }

    public Map<String, Long> getSentinelUsages() {
        return sentinelUsages;
    }
}
