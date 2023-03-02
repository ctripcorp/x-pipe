package com.ctrip.xpipe.redis.checker.model;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DcPriority {

    private String dc;
    private Map<Integer, List<String>> priority2Dcs = new TreeMap<>();

    public String getDc() {
        return dc;
    }

    public DcPriority setDc(String dc) {
        this.dc = dc;
        return this;
    }

    public Map<Integer, List<String>> getPriority2Dcs() {
        return priority2Dcs;
    }

    public void addPriorityAndDc(Integer priority, String targetDc) {
        List<String> existed = priority2Dcs.get(priority);
        if (existed == null)
            priority2Dcs.put(priority, Lists.newArrayList(targetDc));
        else existed.add(targetDc);
    }

    @Override
    public String toString() {
        return "DcPriority{" +
                "dc='" + dc + '\'' +
                ", priority2Dcs=" + priority2Dcs +
                '}';
    }
}
