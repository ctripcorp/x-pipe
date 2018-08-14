package com.ctrip.xpipe.redis.console.redis;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */
public interface SentinelManager {

    void removeShardSentinelMonitors(ShardEvent shardInfo);

    void removeSentinelMonitor(Sentinel sentinel, String sentinelMonitorName);

    HostPort getMasterOfMonitor(Sentinel sentinel, String sentinelMonitorName);

    String infoSentinel(Sentinel sentinel);

    void monitorMaster(Sentinel sentinel, String sentinelMonitorName, HostPort master, int quorum);

    List<HostPort> slaves(Sentinel sentinel, String sentinelMonitorName);

    void reset(Sentinel sentinel, String sentinelMonitorName);
}
