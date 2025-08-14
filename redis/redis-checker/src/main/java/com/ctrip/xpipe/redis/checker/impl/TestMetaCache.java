package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import org.unidal.tuple.Triple;

import java.util.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public class TestMetaCache implements MetaCache {

    XpipeMeta xpipeMeta = new XpipeMeta();

    public TestMetaCache(){
    }

    public void setXpipeMeta(XpipeMeta xpipeMeta) {
        this.xpipeMeta = xpipeMeta;
    }

    @Override
    public XpipeMeta getXpipeMeta() {
        return xpipeMeta;
    }

    @Override
    public XpipeMeta getXpipeMetaLongPull(long version) throws InterruptedException {
        return null;
    }

    @Override
    public XpipeMeta getDividedXpipeMeta(int partIndex) {
        return xpipeMeta;
    }

    @Override
    public XpipeMetaManager.MetaDesc findMetaDesc(HostPort hostPort) {
        return null;
    }

    @Override
    public String getXmlFormatDividedXpipeMeta(int partIndex) {
        return xpipeMeta.toString();
    }

    @Override
    public boolean inBackupDc(HostPort hostPort) {
        return true;
    }

    @Override
    public HostPort findMasterInSameShard(HostPort hostPort) {
        return null;
    }

    @Override
    public Set<HostPort> getAllKeepers() {
        return null;
    }

    @Override
    public String getDcByIpAndPeerClusterShard(String hostIp, Pair<String, String> peerClusterShard) {
        return null;
    }

    @Override
    public Map<String, String> getAllKeeperContainersDcMap() {
        return null;
    }

    @Override
    public Pair<String, String> findClusterShard(HostPort hostPort) {
        return null;
    }

    @Override
    public Map<String, String> getAllApplierContainersDcMap() {
        return null;
    }

    @Override
    public String getSentinelMonitorName(String clusterId, String shardId) {
        return null;
    }

    @Override
    public Set<HostPort> getActiveDcSentinels(String clusterId, String shardId) {
        return null;
    }

    @Override
    public Set<HostPort> getAllSentinels() {
        return null;
    }

    @Override
    public HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException {
        return null;
    }

    @Override
    public String getDc(HostPort hostPort) {
        return "oy";
    }

    @Override
    public String getDcByIpAndClusterShard(String hostIp, Pair<String, String> clusterShard) {
        return null;
    }

    @Override
    public Triple<String, String, Long> findClusterShardBySentinelMonitor(String monitor) {
        return null;
    }

    @Override
    public List<RouteMeta> getCurrentDcConsoleRoutes() {
        return null;
    }

    @Override
    public Map<String, RouteMeta> chooseDefaultMetaRoutes(String clusterName, String srcDc, List<String> dstDcs) {
        return null;
    }

    @Override
    public Map<String, RouteMeta> chooseClusterMetaRoutes(String clusterName, String backUpDcName, List<String> peerDcs) {
        return null;
    }

    @Override
    public boolean isCrossRegion(String activeDc, String backupDc) {
        return false;
    }

    @Override
    public boolean isCurrentDc(String dc) {
        return false;
    }

    @Override
    public List<HostPort> getAllActiveRedisOfDc(String activeDc, String dcId) {
        return null;
    }

    public String getActiveDc(String clusterId){return null;}

    @Override
    public boolean isDcInRegion(String dc, String zone) {
        return true;
    }

    @Override
    public String getActiveDc(HostPort hostPort) {
        return null;
    }

    @Override
    public ClusterType getAzGroupType(HostPort hostPort) {
        return null;
    }

    @Override
    public long getLastUpdateTime() {
        return 0;
    }

    @Override
    public long getVersion() {
        return xpipeMeta.getVersion();
    }

    @Override
    public ClusterType getClusterType(String clusterId) {
        return ClusterType.ONE_WAY;
    }

    @Override
    public boolean isMetaChain(HostPort src, HostPort dst) {
        return false;
    }

    @Override
    public Map<Long, String> dcShardIds(String clusterId, String dcId) {
        return null;
    }

    @Override
    public List<RedisMeta> getRedisOfDcClusterShard(String dc, String cluster, String shard) {
        return Collections.emptyList();
    }

    @Override
    public List<KeeperMeta> getKeeperOfDcClusterShard(String dc, String cluster, String shard) {
        return null;
    }

    @Override
    public List<RedisMeta> getSlavesOfShard(String cluster, String shard) {
        return null;
    }

    @Override
    public List<RedisMeta> getSlavesOfDcClusterShard(String dc, String cluster, String shard) {
        return null;
    }

    @Override
    public List<RedisMeta> getAllInstancesOfShard(String cluster, String shard) {
        return null;
    }

    @Override
    public List<RedisMeta> getAllInstanceOfDc(String cluster, String dc) {
        return null;
    }

    @Override
    public boolean isAsymmetricCluster(String clusterName) {
        return false;
    }

    @Override
    public Set<String> getAllShardNamesByClusterName(String clusterName) {
        return Collections.emptySet();
    }

    @Override
    public Map<String, Integer> getClusterCntMap(String clusterName) {
        return Collections.emptyMap();
    }

    @Override
    public boolean isDcClusterMigratable(String clusterName, String dc) {
        return false;
    }

    @Override
    public boolean anyDcMigratable(String clusterName) {
        return false;
    }

    @Override
    public int getMigratableClustersCountByActiveDc(String activeDc) {
        return 0;
    }

    @Override
    public Map<String, Integer> getAllDcMigratableClustersCnt() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, List<RedisMeta>> getAllInstance(String cluster) {
        return new HashMap<>();
    }
    @Override
    public boolean isBackupDcAndCrossRegion(String currentDc, String activeDc, List<String> dcs) {
        return false;
    }

}
