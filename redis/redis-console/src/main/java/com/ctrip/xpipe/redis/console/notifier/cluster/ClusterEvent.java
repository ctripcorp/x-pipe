package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.api.observer.Event;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */
public interface ClusterEvent extends Event, Observable {

    List<ShardEvent> getShardEvents();

    EventType getClusterEventType();

    String getClusterName();

    long getOrgId();

    void addShardEvent(ShardEvent shardEvent);

    void onEvent();

    ClusterType getClusterType();
}
