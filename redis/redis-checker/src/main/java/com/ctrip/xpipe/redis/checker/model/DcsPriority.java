package com.ctrip.xpipe.redis.checker.model;

import java.util.HashMap;
import java.util.Map;


public class DcsPriority {

    private Map<String, DcPriority> dcPriorityMap = new HashMap<>();

    public DcsPriority setDcPriorityMap(Map<String, DcPriority> dcPriorityMap) {
        this.dcPriorityMap = dcPriorityMap;
        return this;
    }

    public DcPriority getDcPriority(String dc) {
        return dcPriorityMap.get(dc);
    }


}
