package com.ctrip.xpipe.redis.console.model;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Jan 22, 2018
 */
public class SentinelUsageModel {

    private String dcName;

    private Map<String, Long> sentinelUsages;

    private Map<String, Map<String, Long>> sentinelTag;

    public SentinelUsageModel(String dcName) {
        this.dcName = dcName;
        sentinelUsages = Maps.newHashMap();
        sentinelTag = Maps.newHashMap();
    }

    public SentinelUsageModel(String dcName, int n) {
        this.dcName = dcName;
        this.sentinelUsages = Maps.newHashMapWithExpectedSize(n);
        this.sentinelTag = Maps.newHashMapWithExpectedSize(n);
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

    public void addSentinelTag(String tag, String sentinelAddress, long usage) {
        if (!sentinelTag.containsKey(tag)) {
            sentinelTag.put(tag, new HashMap<>());
        }
        sentinelTag.get(tag).put(sentinelAddress, usage);
    }

    public Map<String, Map<String, Long>> getSentinelTag() {
        return sentinelTag;
    }
}
