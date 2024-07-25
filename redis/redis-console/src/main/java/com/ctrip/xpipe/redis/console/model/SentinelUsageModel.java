package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Jan 22, 2018
 */
public class SentinelUsageModel {

    private String dcName;

    private Map<String, Pair<Long, String>> sentinelUsages;

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

    public SentinelUsageModel addSentinelUsage(String sentinelAddress, long usage, String tag) {
        this.sentinelUsages.put(sentinelAddress, new Pair<>(usage, tag));
        return this;
    }

    public Map<String, Pair<Long, String>> getSentinelUsages() {
        return sentinelUsages;
    }
}
