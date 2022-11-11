package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HeteroDelayActionContexts {

    private Map<Long, HeteroDelayActionContext> contexts = new ConcurrentHashMap<>();

    public Map<Long, HeteroDelayActionContext> getContexts() {
        return contexts;
    }

    public void addContext(HeteroDelayActionContext context) {
        this.contexts.put(context.getShardDbId(),context);
    }
}
