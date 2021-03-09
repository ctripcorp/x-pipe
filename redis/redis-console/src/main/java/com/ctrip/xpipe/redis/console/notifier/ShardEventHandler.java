package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;

/**
 * @author lishanglin
 * date 2021/3/10
 */
public interface ShardEventHandler {

    void handleShardDelete(ShardEvent shardEvent);

}
