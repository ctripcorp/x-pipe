package com.ctrip.xpipe.redis.console.notifier.shard;

import com.ctrip.xpipe.redis.console.notifier.EventType;

import java.util.concurrent.ExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Feb 08, 2018
 */
public class ShardDeleteEvent extends AbstractShardEvent {

    public ShardDeleteEvent() {
        super();
    }

    public ShardDeleteEvent(String clusterName, String shardName, ExecutorService executor) {
        super(clusterName, shardName, executor);
    }

    @Override
    public EventType getShardEventType() {
        return EventType.DELETE;
    }

    @Override
    protected ShardEvent getSelf() {
        return ShardDeleteEvent.this;
    }
}
