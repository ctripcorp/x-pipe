package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.redis.console.notifier.EventType;

import java.util.concurrent.ExecutorService;

public class ClusterTypeUpdateEvent extends AbstractClusterEvent {

    protected ClusterTypeUpdateEvent(String clusterName, long orgId, ExecutorService executor) {
        super(clusterName, orgId, executor);
    }

    @Override
    protected ClusterEvent getSelf() {
        return ClusterTypeUpdateEvent.this;
    }

    @Override
    public EventType getClusterEventType() {
        return EventType.UPDATE;
    }

}
