package com.ctrip.xpipe.redis.console.redis;

import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */
public interface SentinelManager {
    void removeShardSentinelMonitors(ShardEvent shardInfo);
}
