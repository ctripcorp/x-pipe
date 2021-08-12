package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.service.meta.KeepercontainerMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public abstract class AbstractMetaCache implements MetaCache {

    protected int DEFAULT_KEEPER_NUMBERS = 3 * 10000;

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected static final String CURRENT_IDC = FoundationService.DEFAULT.getDataCenter();

    protected Pair<XpipeMeta, XpipeMetaManager> meta;

    protected Map<String, Pair<String, String>> monitor2ClusterShard;

    protected Set<HostPort> allKeepers;

    protected int allKeeperSize = DEFAULT_KEEPER_NUMBERS;

    protected long lastUpdateTime = 0;

    @Override
    public XpipeMeta getXpipeMeta() {
        try {
            return meta.getKey();
        } catch (Exception e) {
            logger.debug("[getXpipeMeta]", e);
        }
        return null;
    }

    @Override
    public XpipeMeta getDividedXpipeMeta(int partIndex) {
        throw new UnsupportedOperationException();
    }

    protected void refreshMeta(XpipeMeta xpipeMeta) {
        Pair<XpipeMeta, XpipeMetaManager> meta = new Pair<>(xpipeMeta, new DefaultXpipeMetaManager(xpipeMeta));
        AbstractMetaCache.this.meta = meta;
        monitor2ClusterShard = Maps.newHashMap();
        allKeeperSize = allKeepers == null ? DEFAULT_KEEPER_NUMBERS : allKeepers.size();
        allKeepers = null;
        lastUpdateTime = System.currentTimeMillis();
    }

    protected XpipeMeta createXpipeMeta(List<DcMeta> dcMetas){

        XpipeMeta xpipeMeta = new XpipeMeta();
        for (DcMeta dcMeta : dcMetas) {
            xpipeMeta.addDc(dcMeta);
        }
        return xpipeMeta;

    }

    protected XpipeMeta createDividedMeta(XpipeMeta full, Set<String> reqClusters) {
        XpipeMeta part = new XpipeMeta();
        for (DcMeta dcMeta: full.getDcs().values()) {
            DcMeta partDcMeta = new DcMeta(dcMeta.getId()).setLastModifiedTime(dcMeta.getLastModifiedTime()).setZone(dcMeta.getZone());
            part.addDc(partDcMeta);

            dcMeta.getClusters().values().stream().filter(clusterMeta -> reqClusters.contains(clusterMeta.getId()))
                    .forEach(partDcMeta::addCluster);
            dcMeta.getSentinels().values().forEach(partDcMeta::addSentinel);
            dcMeta.getKeeperContainers().forEach(partDcMeta::addKeeperContainer);
            dcMeta.getRoutes().forEach(partDcMeta::addRoute);
            dcMeta.getMetaServers().forEach(partDcMeta::addMetaServer);
        }

        return part;
    }

    @Override
    public boolean inBackupDc(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }

        String instanceInDc = metaDesc.getDcId();
        String activeDc = metaDesc.getActiveDc();
        return !activeDc.equalsIgnoreCase(instanceInDc);
    }

    @Override
    public HostPort findMasterInSameShard(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }

        String clusterName = metaDesc.getClusterId();
        String shardName = metaDesc.getShardId();

        Pair<String, RedisMeta> redisMaster = xpipeMetaManager.getRedisMaster(clusterName, shardName);
        // could be null if no master in a shard
        if(redisMaster == null) {
            return null;
        }
        RedisMeta redisMeta = redisMaster.getValue();
        return new HostPort(redisMeta.getIp(), redisMeta.getPort());
    }

    @Override
    public Pair<String, String> findClusterShard(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            return null;
        }

        return new Pair<>(metaDesc.getClusterId(), metaDesc.getShardId());
    }

    @Override
    public String getActiveDc(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            return null;
        }

        return metaDesc.getActiveDc();
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public RouteMeta getRouteIfPossible(HostPort hostPort) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            logger.warn("[getRouteIfPossible]HostPort corresponding meta not found: {}", hostPort);
            return null;
        }
        return xpipeMetaManager
                .consoleRandomRoute(CURRENT_IDC, XpipeMetaManager.ORG_ID_FOR_SHARED_ROUTES, metaDesc.getDcId());
    }

    @Override
    public boolean isCrossRegion(String activeDc, String backupDc) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return !xpipeMetaManager.getDcZone(activeDc)
                .equalsIgnoreCase(xpipeMetaManager.getDcZone(backupDc));
    }

    @Override
    public List<HostPort> getAllActiveRedisOfDc(String activeDc, String dcId) {
        List<HostPort> result = Lists.newLinkedList();
        boolean isDcActiveDc = activeDc.equalsIgnoreCase(dcId);
        try {
            for(ClusterMeta clusterMeta : meta.getKey().findDc(dcId).getClusters().values()) {
                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (clusterType.supportSingleActiveDC() && !clusterMeta.getActiveDc().equalsIgnoreCase(activeDc)) {
                    continue;
                }
                if (clusterType.supportMultiActiveDC() && !isDcActiveDc) {
                    continue;
                }
                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    for(RedisMeta redis : shardMeta.getRedises()) {
                        result.add(new HostPort(redis.getIp(), redis.getPort()));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("[getRedisNumOfDc]", e);
        }
        return result;
    }

    @Override
    public Set<HostPort> getAllKeepers(){
        XpipeMeta xpipeMeta = getXpipeMeta();
        if (allKeepers == null) {
            synchronized (this) {
                if (allKeepers == null) {
                    Set<HostPort> localKeepers = Sets.newHashSetWithExpectedSize(allKeeperSize);
                    xpipeMeta.getDcs().forEach((dcName, dcMeta) -> {
                        dcMeta.getClusters().forEach((clusterName, clusterMeta) -> {
                            clusterMeta.getShards().forEach((shardName, shardMeta) -> {
                                shardMeta.getKeepers().forEach(keeperMeta -> {
                                    localKeepers.add(new HostPort(keeperMeta.getIp(), keeperMeta.getPort()));
                                });
                            });
                        });
                    });
                    allKeepers = localKeepers;
                }
            }
        }

        return allKeepers;
    }


    @Override
    public String getSentinelMonitorName(String clusterId, String shardId) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        String activeDc = xpipeMetaManager.getActiveDc(clusterId, shardId);
        return xpipeMetaManager.getSentinelMonitorName(activeDc, clusterId, shardId);
    }

    @Override
    public Set<HostPort> getActiveDcSentinels(String clusterId, String shardId) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        String activeDc = xpipeMetaManager.getActiveDc(clusterId, shardId);
        SentinelMeta sentinel = xpipeMetaManager.getSentinel(activeDc, clusterId, shardId);

        return new HashSet<>(IpUtils.parseAsHostPorts(sentinel.getAddress()));
    }

    @Override
    public HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        Pair<String, RedisMeta> redisMaster = xpipeMetaManager.getRedisMaster(clusterId, shardId);
        if (redisMaster == null) {
            throw new MasterNotFoundException(clusterId, shardId);
        }
        return new HostPort(redisMaster.getValue().getIp(), redisMaster.getValue().getPort());
    }

    @Override
    public String getDc(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);

        if (metaDesc == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }
        return metaDesc.getDcId();
    }

    @Override
    public Pair<String, String> findClusterShardBySentinelMonitor(String monitor) {
        if(StringUtil.isEmpty(monitor)) {
            return null;
        }

        Pair<String, String> clusterShard = monitor2ClusterShard.get(monitor);
        if(clusterShard != null) {
            return clusterShard;
        }

        synchronized (this) {
            loadSentinelMonitorInfo();
        }
        return monitor2ClusterShard.get(monitor);
    }

    private void loadSentinelMonitorInfo() {
        try {
            XpipeMeta xpipeMeta = meta.getKey();
            for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
                for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                    ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                    if (clusterType.supportSingleActiveDC() && !clusterMeta.getActiveDc().equals(dcMeta.getId())) {
                        continue;
                    }

                    for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                        monitor2ClusterShard.put(shardMeta.getSentinelMonitorName(),
                                new Pair<>(clusterMeta.getId(), shardMeta.getId()));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("[loadSentinelMonitorInfo]", e);
            throw e;
        }
    }

    @Override
    public String getActiveDc(String clusterId, String shardId){
        XpipeMetaManager xpipeMetaManager  =  meta.getValue();
        return xpipeMetaManager.getActiveDc(clusterId, shardId);
    }

    @Override
    public ClusterType getClusterType(String clusterId) {
        XpipeMeta xpipeMeta = meta.getKey();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!dcMeta.getClusters().containsKey(clusterId)) continue;
            return ClusterType.lookup(dcMeta.getClusters().get(clusterId).getType());
        }

        throw new IllegalStateException("[getClusterType] unfound cluster for name:" + clusterId);
    }

    @Override
    public boolean isMetaChain(HostPort chainSrc, HostPort chainDst) {
        Pair<String,String> clusterShard = findClusterShard(chainDst);
        ClusterType clusterType = getClusterType(clusterShard.getKey());

        // currently chainSrc contains random port, which can't be used to validate if chainSrc
        // is a keeper (for ONE_WAY) or a redis (for BI_DIRECTION).
        // TODO: check chainSrc if keeper & redis FORWARD_FOR their listening port
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc dstDesc = xpipeMetaManager.findMetaDesc(chainDst);
        if (dstDesc == null) return false;
        Redis redis = dstDesc.getRedis();
        if (redis == null) return false;

        if ((clusterType.equals(ClusterType.ONE_WAY) && (redis instanceof KeeperMeta)) ||
                (clusterType.equals(ClusterType.BI_DIRECTION) && (redis instanceof RedisMeta))) {
            return true;
        }
        return false;
    }
    @VisibleForTesting
    protected AbstractMetaCache setMeta(Pair<XpipeMeta, XpipeMetaManager> meta) {
        this.meta = meta;
        return this;
    }

    @VisibleForTesting
    protected AbstractMetaCache setMonitor2ClusterShard(Map<String, Pair<String, String>> monitorMap) {
        this.monitor2ClusterShard = monitorMap;
        return this;
    }
}
