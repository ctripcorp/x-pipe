package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public interface MetaCache {

    XpipeMeta getXpipeMeta();

    XpipeMeta getDividedXpipeMeta(int partIndex);

    XpipeMetaManager.MetaDesc findMetaDesc(HostPort hostPort);

    String getXmlFormatDividedXpipeMeta(int partIndex);

    boolean inBackupDc(HostPort hostPort);

    HostPort findMasterInSameShard(HostPort hostPort);

    Set<HostPort> getAllKeepers();

    String getDcByIpAndPeerClusterShard(String hostIp, Pair<String, String> peerClusterShard);

    Map<String, String> getAllKeeperContainersDcMap();

    Pair<String, String> findClusterShard(HostPort hostPort);

    Map<String, String> getAllApplierContainersDcMap();

    String getSentinelMonitorName(String clusterId, String shardId);

    Set<HostPort> getActiveDcSentinels(String clusterId, String shardId);

    HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException;

    List<RedisMeta> getRedisOfDcClusterShard(String dc, String cluster, String shard);

    List<KeeperMeta> getKeeperOfDcClusterShard(String dc, String cluster, String shard);

    List<RedisMeta> getSlavesOfDcClusterShard(String dc, String cluster, String shard);

    List<RedisMeta> getSlavesOfShard(String cluster, String shard);

    List<RedisMeta> getAllInstancesOfShard(String cluster, String shard);

    String getDc(HostPort hostPort);

    String getDcByIpAndClusterShard(String hostIp, Pair<String, String> clusterShard);

    Pair<String, String> findClusterShardBySentinelMonitor(String monitor);

    List<RouteMeta> getCurrentDcConsoleRoutes();

    Map<String, RouteMeta> chooseDefaultMetaRoutes(String clusterName, String srcDc, List<String> dstDcs);

    Map<String, RouteMeta> chooseClusterMetaRoutes(String clusterName, String srcDc, List<String> dstDcs);

    boolean isCrossRegion(String activeDc, String backupDc);

    boolean isDcInRegion(String dc, String zone);

    // get all redis from dc whose health status is visible to activeDc
    List<HostPort> getAllActiveRedisOfDc(String activeDc, String dcId);

    String getActiveDc(String clusterId);

    String getActiveDc(HostPort hostPort);

    ClusterType getAzGroupType(HostPort hostPort);

    long getLastUpdateTime();

    ClusterType getClusterType(String clusterId);

    boolean isMetaChain(HostPort src, HostPort dst);

    Map<Long, String> dcShardIds(String clusterId, String dcId);

    boolean isAsymmetricCluster(String clusterName);
}
