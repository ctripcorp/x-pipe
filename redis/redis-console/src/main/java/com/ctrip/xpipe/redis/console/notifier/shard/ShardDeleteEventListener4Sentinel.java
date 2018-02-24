package com.ctrip.xpipe.redis.console.notifier.shard;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.redis.SentinelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Feb 09, 2018
 */
@Component
public class ShardDeleteEventListener4Sentinel implements ShardDeleteEventListener {

    private static Logger logger = LoggerFactory.getLogger(ShardDeleteEventListener4Sentinel.class);

    @Autowired
    private SentinelManager sentinelManager;

    @Override
    public void update(Object args, Observable observable) {
        EventType type = (EventType) args;
        if(!(observable instanceof ShardDeleteEvent) || type != EventType.DELETE) {
            logger.info("[update] observable object not ShardDeleteEvent, skip. observable: {}, args: {}",
                    observable.getClass().getName(),
                    args.getClass().getName());
            return;
        }
        ShardDeleteEvent shardDeleteEvent = (ShardDeleteEvent) observable;

        sentinelManager.removeShardSentinelMonitors(shardDeleteEvent);
    }


}
