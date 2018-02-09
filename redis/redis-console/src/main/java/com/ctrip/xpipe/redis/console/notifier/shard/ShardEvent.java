package com.ctrip.xpipe.redis.console.notifier.shard;

import com.ctrip.xpipe.api.observer.Event;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.console.notifier.EventType;

/**
 * @author chen.zhu
 * <p>
 * Feb 08, 2018
 */
public interface ShardEvent extends Event, Observable {

    EventType getShardEventType();

    String getShardName();

    String getClusterName();

    String getShardSentinels();

    String getShardMonitorName();

    void onEvent();
}
