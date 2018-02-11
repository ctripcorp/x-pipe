package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */
public class ClusterDeleteEvent extends AbstractClusterEvent {

    protected ClusterDeleteEvent(String clusterName, ExecutorService executor) {
        super(clusterName, executor);
    }

    @Override
    protected ClusterEvent getSelf() {
        return ClusterDeleteEvent.this;
    }

    @Override
    public EventType getClusterEventType() {
        return EventType.DELETE;
    }

}
