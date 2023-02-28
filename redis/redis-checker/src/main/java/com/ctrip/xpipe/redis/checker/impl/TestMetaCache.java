package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public class TestMetaCache implements MetaCache {

    XpipeMeta xpipeMeta = new XpipeMeta();

    public TestMetaCache(){
    }

    @Override
    public XpipeMeta getXpipeMeta() {
        return xpipeMeta;
    }

    @Override
    public XpipeMeta getDividedXpipeMeta(int partIndex) {
        return xpipeMeta;
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
    public Pair<String, String> findClusterShard(HostPort hostPort) {
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
    public HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException {
        return null;
    }

    @Override
    public String getDc(HostPort hostPort) {
        return "oy";
    }

    @Override
    public Pair<String, String> findClusterShardBySentinelMonitor(String monitor) {
        return null;
    }

    @Override
    public List<RouteMeta> getRoutes() {
        return null;
    }

    @Override
    public Map<String, RouteMeta> chooseDefaultRoutes(String clusterName, String srcDc, List<String> dstDcs, int orgId) {
        return null;
    }

    @Override
    public Map<String, RouteMeta> chooseRoutes(String clusterName, String backUpDcName, List<String> peerDcs, int orgId) {
        return null;
    }

    @Override
    public boolean isCrossRegion(String activeDc, String backupDc) {
        return false;
    }

    @Override
    public List<HostPort> getAllActiveRedisOfDc(String activeDc, String dcId) {
        return null;
    }

    public String getActiveDc(String clusterId, String shardId){return null;}

    @Override
    public String getActiveDc(HostPort hostPort) {
        return null;
    }

    @Override
    public String getDcGroupType(HostPort hostPort) {
        return null;
    }

    @Override
    public long getLastUpdateTime() {
        return 0;
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
    public boolean isHeteroCluster(String clusterName) {
        return false;
    }
}
