package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.tuple.Pair;
import org.unidal.tuple.Triple;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <b>按 {@link HostPort} 查找的方法，入参是 meta 地址</b>：redis 与物理机 keeper 为 {@code IP:port}，
 * 容器 keeper 为 {@code hostname:port}，不接受容器的网络 IP。持有网络地址的调用方需先自行翻译
 * （如 console 的 {@code DefaultProxyChainAnalyzer} 经 {@code CrossDcKeeperContainerAddressService.findHostnamesByIp}）。
 *
 * <p><b>容器 keeper 的 {@code ip} 字段</b>：DB 与 Console 自身缓存中是占位值 {@code 0.0.0.0}；
 * Console 下发给 Checker 时在接口出口将其填充为真实网络 IP（copy-on-write，不影响 Console 缓存），
 * Checker 不再 watch ZK 反查，读 {@code keeperMeta.getIp()} 即为可直连地址。
 * 该填充不改变本接口按 HostPort 查找的上述约定。
 *
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public interface MetaCache {

    XpipeMeta getXpipeMeta();

    XpipeMeta getXpipeMetaLongPull(long version) throws InterruptedException;

    XpipeMeta getDividedXpipeMeta(int partIndex);

    /**
     * @param hostPort meta 地址（容器 keeper 为 {@code hostname:port}，见类注释）
     * @return 未命中返回 null
     */
    XpipeMetaManager.MetaDesc findMetaDesc(HostPort hostPort);

    String getXmlFormatDividedXpipeMeta(int partIndex);

    /**
     * @param hostPort meta 地址（容器 keeper 为 {@code hostname:port}，见类注释）
     */
    boolean inBackupDc(HostPort hostPort);

    /**
     * @param hostPort meta 地址（容器 keeper 为 {@code hostname:port}，见类注释）
     */
    HostPort findMasterInSameShard(HostPort hostPort);

    /**
     * @param hostPort meta 地址（容器 keeper 为 {@code hostname:port}，见类注释）
     * @return {@code (cluster, shard)}；未命中返回 null
     */
    Pair<String, String> findClusterShard(HostPort hostPort);

    String getSentinelMonitorName(String clusterId, String shardId);

    Set<HostPort> getActiveDcSentinels(String clusterId, String shardId);

    Set<HostPort> getAllSentinels();

    HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException;

    List<RedisMeta> getRedisOfDcClusterShard(String dc, String cluster, String shard);

    List<KeeperMeta> getKeeperOfDcClusterShard(String dc, String cluster, String shard);

    /**
     * Keeper 所属 KeeperContainer 的磁盘类型（静态配置，非实时状态）。
     *
     * @return 未命中 keeper 或其 container 时返回 null
     */
    String getKeeperDiskType(String dc, KeeperMeta keeperMeta);

    Map<KeeperMeta, String> getKeeper2DcOfClusterShard(String cluster, String shard);

    List<RedisMeta> getSlavesOfDcClusterShard(String dc, String cluster, String shard);

    List<RedisMeta> getSlavesOfShard(String cluster, String shard);

    List<RedisMeta> getAllInstancesOfShard(String cluster, String shard);

    List<RedisMeta> getAllInstanceOfDc(String cluster, String dc);

    Map<String, List<RedisMeta>> getAllInstance(String cluster);

    /**
     * @param hostPort meta 地址（容器 keeper 为 {@code hostname:port}，见类注释）
     */
    String getDc(HostPort hostPort);

    String getDcByIpAndClusterShard(String hostIp, Pair<String, String> clusterShard);

    Triple<String, String, Long> findClusterShardBySentinelMonitor(String monitor);

    List<RouteMeta> getCurrentDcConsoleRoutes();

    Map<String, RouteMeta> chooseDefaultMetaRoutes(String clusterName, String srcDc, List<String> dstDcs);

    Map<String, RouteMeta> chooseClusterMetaRoutes(String clusterName, String srcDc, List<String> dstDcs);

    boolean isCrossRegion(String activeDc, String backupDc);

    String getDcZone(String dc);

    boolean isCurrentDc(String dc);

    boolean isDcInRegion(String dc, String zone);

    // get all redis from dc whose health status is visible to activeDc
    List<HostPort> getAllActiveRedisOfDc(String activeDc, String dcId);

    String getActiveDc(String clusterId);

    /**
     * @param hostPort meta 地址（容器 keeper 为 {@code hostname:port}，见类注释）
     */
    String getActiveDc(HostPort hostPort);

    /**
     * @param hostPort meta 地址（容器 keeper 为 {@code hostname:port}，见类注释）
     */
    ClusterType getAzGroupType(HostPort hostPort);

    long getLastUpdateTime();

    long getVersion();

    ClusterType getClusterType(String clusterId);

    /**
     * @param src 链路源端网络地址（端口为随机端口）
     * @param dst 链路目的端 meta 地址（容器 keeper 为 {@code hostname:port}，见类注释）
     */
    boolean isMetaChain(HostPort src, HostPort dst);

    Map<Long, String> dcShardIds(String clusterId, String dcId);

    Set<String> getAllShardNamesByClusterName(String clusterName);

    Map<String, Integer> getClusterCntMap(String clusterName);

    Map<String, Map<String, Integer>> getClusterShardCounts();

    boolean isDcClusterMigratable(String dc, String clusterName);

    boolean anyDcMigratable(String clusterName);

    int getMigratableClustersCountByActiveDc(String activeDc);

    Map<String, Integer> getAllDcMigratableClustersCnt();

    boolean isBackupDcAndCrossRegion(String currentDc, String activeDc, List<String> backupDcs);

    List<String> currentRegionDcs();
}
