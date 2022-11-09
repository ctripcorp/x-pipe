package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.RedisDelayManager;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface DelayService extends RedisDelayManager {

    void updateRedisDelays(Map<HostPort, Long> redisDelays);

    void updateHeteroShardsDelays(Map<Long, Long> heteroShardsDelays);

    long getShardDelay(String clusterId, String shardId, Long shardDbId);

    long getDelay(HostPort hostPort);

    long getDelay(ClusterType clusterType, HostPort hostPort);

    long getLocalCachedDelay(HostPort hostPort);

    long getLocalCachedShardDelay(long shardId);

    Map<HostPort, Long> getDcCachedDelay(String dc);

    UnhealthyInfoModel getDcActiveClusterUnhealthyInstance(String dc);

    UnhealthyInfoModel getAllUnhealthyInstance();

    UnhealthyInfoModel getAllUnhealthyInstanceFromParallelService();
}
