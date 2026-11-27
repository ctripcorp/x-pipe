package com.ctrip.xpipe.redis.console.service.logicalbu;

import java.util.ArrayList;
import java.util.List;

/** Body of POST /console/logical-bus/switchBM and /console/logical-bus/switchRestore. */
public class KeeperOpRequest {

    private List<Long> logicalBuIds = new ArrayList<>();

    public List<Long> getLogicalBuIds() {
        return logicalBuIds;
    }

    public KeeperOpRequest setLogicalBuIds(List<Long> logicalBuIds) {
        this.logicalBuIds = logicalBuIds;
        return this;
    }
}