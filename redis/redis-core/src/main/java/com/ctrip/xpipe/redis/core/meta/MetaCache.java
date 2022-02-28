package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public interface MetaCache {

    XpipeMeta getXpipeMeta();

    XpipeMeta getDividedXpipeMeta(int partIndex);

    boolean inBackupDc(HostPort hostPort);

    HostPort findMasterInSameShard(HostPort hostPort);

    Set<HostPort> getAllKeepers();

    Pair<String, String> findClusterShard(HostPort hostPort);

    String getSentinelMonitorName(String clusterId, String shardId);

    Set<HostPort> getActiveDcSentinels(String clusterId, String shardId);

    HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException;

    List<RedisMeta> getRedisOfDcClusterShard(String dc, String cluster, String shard);

    List<RedisMeta> getSlavesOfShard(String cluster, String shard);

    String getDc(HostPort hostPort);

    Pair<String, String> findClusterShardBySentinelMonitor(String monitor);

    List<RouteMeta> getRoutes();

    boolean isCrossRegion(String activeDc, String backupDc);

    // get all redis from dc whose health status is visible to activeDc
    List<HostPort> getAllActiveRedisOfDc(String activeDc, String dcId);

    String getActiveDc(String clusterId, String shardId);

    String getActiveDc(HostPort hostPort);

    long getLastUpdateTime();

    ClusterType getClusterType(String clusterId);

    boolean isMetaChain(HostPort src, HostPort dst);

    Pair<String, Integer> getMaxMasterCountDc(String clusterName, Set<String> excludedDcs);
}
