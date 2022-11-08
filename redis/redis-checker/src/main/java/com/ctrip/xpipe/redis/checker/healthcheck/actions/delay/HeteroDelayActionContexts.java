package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import java.util.HashSet;
import java.util.Set;

public class HeteroDelayActionContexts {

    private Set<HeteroDelayActionContext> contexts=new HashSet<>();

    public Set<HeteroDelayActionContext> getContexts() {
        return contexts;
    }

    public void addContext(HeteroDelayActionContext context) {
        this.contexts.add(context);
    }
}
