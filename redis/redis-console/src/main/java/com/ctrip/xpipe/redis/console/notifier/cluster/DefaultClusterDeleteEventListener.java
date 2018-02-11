package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEventListener4Sentinel;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */
@Component
public class DefaultClusterDeleteEventListener implements ClusterDeleteEventListener {

    private static Logger logger = LoggerFactory.getLogger(ShardDeleteEventListener4Sentinel.class);

    @Override
    public void update(Object args, Observable observable) {
        EventType type = (EventType) args;
        if(!(observable instanceof ClusterDeleteEvent) || type != EventType.DELETE) {
            logger.info("[update] observable object not ShardDeleteEvent, skip. observable: {}, args: {}",
                    observable.getClass().getName(),
                    args.getClass().getName());
            return;
        }
        ClusterDeleteEvent clusterDeleteEvent = (ClusterDeleteEvent) observable;

        for(ShardEvent shardEvent : clusterDeleteEvent.getShardEvents()) {
            shardEvent.onEvent();
        }
    }
}
