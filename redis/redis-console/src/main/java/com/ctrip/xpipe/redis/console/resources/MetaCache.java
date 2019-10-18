package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
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

    boolean inBackupDc(HostPort hostPort);

    HostPort findMasterInSameShard(HostPort hostPort);

    Set<HostPort> allKeepers();

    Pair<String, String> findClusterShard(HostPort hostPort);

    String getSentinelMonitorName(String clusterId, String shardId);

    Set<HostPort> getActiveDcSentinels(String clusterId, String shardId);

    HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException;

    String getDc(HostPort hostPort);

    Pair<String, String> findClusterShardBySentinelMonitor(String monitor);

    RouteMeta getRouteIfPossible(HostPort hostPort);

    boolean isReplThroughProxy(String activeDc, String backupDc);

    List<HostPort> getAllRedisOfDc(String dcId);

    String getActiveDc(String clusterId, String shardId);
}
