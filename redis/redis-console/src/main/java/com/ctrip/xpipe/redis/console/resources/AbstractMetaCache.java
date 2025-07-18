package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.exception.LoadConsoleMetaException;
import com.ctrip.xpipe.redis.console.exception.TooManyClustersRemovedException;
import com.ctrip.xpipe.redis.console.exception.TooManyDcsRemovedException;
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
import org.unidal.tuple.Triple;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.cluster.ClusterType.HETERO;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public abstract class AbstractMetaCache implements MetaCache {

    protected int DEFAULT_KEEPER_NUMBERS = 3 * 10000;

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected static final String CURRENT_IDC = FoundationService.DEFAULT.getDataCenter();

    protected Pair<XpipeMeta, XpipeMetaManager> meta;

    protected Map<String, Triple<String, String, Long>> monitor2ClusterShard;

    protected Set<HostPort> allKeepers;

    protected Map<String, String> allKeeperContainersDcMap;

    protected Map<String, String> allApplierContainersDcMap;

    protected int allKeeperSize = DEFAULT_KEEPER_NUMBERS;

    protected long lastUpdateTime = 0;

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

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

    @Override
    public XpipeMetaManager.MetaDesc findMetaDesc(HostPort hostPort) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return xpipeMetaManager.findMetaDesc(hostPort);
    }

    @Override
    public String getXmlFormatDividedXpipeMeta(int partIndex) {
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

    protected XpipeMeta createXpipeMeta(List<DcMeta> dcMetas, List<RedisCheckRuleMeta> redisCheckRuleMetas){

        XpipeMeta xpipeMeta = new XpipeMeta();
        for (DcMeta dcMeta : dcMetas) {
            xpipeMeta.addDc(dcMeta);
        }

        setActiveDcForCrossDcClusters(xpipeMeta);

        for(RedisCheckRuleMeta redisCheckRuleMeta : redisCheckRuleMetas) {
            xpipeMeta.addRedisCheckRule(redisCheckRuleMeta);
        }
        xpipeMeta.setVersion(System.currentTimeMillis());
        return xpipeMeta;
    }

    @Override
    public XpipeMeta getXpipeMetaLongPull(long version) throws InterruptedException {
        return null;
    }

    void setActiveDcForCrossDcClusters(XpipeMeta xpipeMeta) {
        try {
            Map<String, Map<String, ClusterMeta>> crossDcClusters = typeDcClusterMap(xpipeMeta, ClusterType.CROSS_DC);

            if (crossDcClusters.isEmpty())
                return;

            crossDcClusters.values().forEach(dcClusters -> {
                Map<String, Integer> dcMasterNums = countDcMaster(dcClusters);
                String activeDc = maxMasterCountDc(dcMasterNums);
                dcClusters.values().forEach(dcCluster -> dcCluster.setActiveDc(activeDc));
            });

        } catch (Throwable e) {
            logger.error("[setActiveDcForCrossDcClusters]", e);
        }
    }

    Map<String, Integer> countDcMaster(Map<String, ClusterMeta> dcClusters) {
        Map<String, Integer> dcMasterCountMap = new HashMap<>();
        dcClusters.forEach((dc, clusterMeta) -> {
            dcMasterCountMap.put(dc, dcMasterCount(clusterMeta));
        });
        return dcMasterCountMap;
    }

    Map<String, Map<String, ClusterMeta>> typeDcClusterMap(XpipeMeta xpipeMeta, ClusterType clusterType) {
        Map<String, Map<String, ClusterMeta>> typeDcClusterMetaMap = new HashMap<>();
        xpipeMeta.getDcs().forEach((dc, dcMeta) -> {
            dcMeta.getClusters().forEach((clusterName, clusterMeta) -> {
                if (ClusterType.lookup(clusterMeta.getType()).equals(clusterType)) {
                    typeDcClusterMetaMap.putIfAbsent(clusterName, new HashMap<>());
                    typeDcClusterMetaMap.get(clusterName).put(dc, clusterMeta.setActiveDc(""));
                }
            });
        });
        return typeDcClusterMetaMap;
    }

    String maxMasterCountDc(Map<String, Integer> dcMasterNumMap) {
        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(dcMasterNumMap.entrySet());
        entryList.sort(new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        return entryList.get(0).getKey();
    }

    private int dcMasterCount(ClusterMeta dcCluster) {
        Map<String, ShardMeta> shards = dcCluster.getShards();
        AtomicInteger masterCount = new AtomicInteger();
        shards.forEach((shardId, shardMeta) -> {
            shardMeta.getRedises().forEach(redisMeta -> {
                if (redisMeta.isMaster()) {
                    masterCount.incrementAndGet();
                }
            });
        });
        return masterCount.get();
    }

    protected XpipeMeta createDividedMeta(XpipeMeta full, Set<String> reqClusters, Set<Long> requestKeeperContainers) {
        XpipeMeta part = new XpipeMeta();
        for (DcMeta dcMeta: full.getDcs().values()) {
            DcMeta partDcMeta = new DcMeta(dcMeta.getId()).setLastModifiedTime(dcMeta.getLastModifiedTime()).setZone(dcMeta.getZone());
            part.addDc(partDcMeta);

            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                if (!reqClusters.contains(clusterMeta.getId())) {
                    continue;
                }

                ClusterMeta partClusterMeta = new ClusterMeta(clusterMeta.getId());
                partClusterMeta.mergeAttributes(clusterMeta);
                clusterMeta.getSources().forEach(partClusterMeta::addSource);
                clusterMeta.getShards().forEach((k, v) -> partClusterMeta.addShard(v));

                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (clusterType == ClusterType.HETERO) {
                    String activeDc = clusterMeta.getActiveDc();
                    String[] backupDcs = clusterMeta.getBackupDcs().split("\\s*,\\s*");
                    Set<String> dcs = new HashSet<>();
                    dcs.add(activeDc);
                    dcs.addAll(Arrays.asList(backupDcs));

                    if (dcs.contains(currentDc)) {
                        ClusterType azGroupType = ClusterType.lookup(clusterMeta.getAzGroupType());
                        partClusterMeta.setType(azGroupType.toString());
                        partClusterMeta.setAzGroupType(null);
                    } else {
                        continue;
                    }
                }

                partDcMeta.addCluster(partClusterMeta);
            }
            dcMeta.getSentinels().values().forEach(partDcMeta::addSentinel);

            dcMeta.getKeeperContainers().forEach(keeperContainerMeta -> {
                if (requestKeeperContainers.contains(keeperContainerMeta.getId())) {
                    partDcMeta.addKeeperContainer(keeperContainerMeta);
                }
            });
            dcMeta.getRoutes().forEach(partDcMeta::addRoute);
            dcMeta.getMetaServers().forEach(partDcMeta::addMetaServer);
        }
        full.getRedisCheckRules().values().forEach(part::addRedisCheckRule);

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
        String azGroupType = metaDesc.getAzGroupType();
        ClusterType azGroupClusterType = StringUtil.isEmpty(azGroupType)
            ? null : ClusterType.lookup(azGroupType);
        return !activeDc.equalsIgnoreCase(instanceInDc) && (azGroupClusterType != ClusterType.SINGLE_DC);
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
    public ClusterType getAzGroupType(HostPort hostPort) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();

        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            return null;
        }

        String azGroupType = metaDesc.getAzGroupType();
        return StringUtil.isEmpty(azGroupType)
            ? null : ClusterType.lookup(azGroupType);
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public long getVersion() {
        if(getXpipeMeta() != null) {
            return getXpipeMeta().getVersion();
        } else {
            return 0l;
        }
    }


    @Override
    public List<RouteMeta> getCurrentDcConsoleRoutes() {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return xpipeMetaManager.consoleRoutes(CURRENT_IDC);
    }

    @Override
    public boolean isCrossRegion(String activeDc, String backupDc) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return !xpipeMetaManager.getDcZone(activeDc)
                .equalsIgnoreCase(xpipeMetaManager.getDcZone(backupDc));
    }

    @Override
    public boolean isCurrentDc(String dc) {
        return currentDc.equalsIgnoreCase(dc);
    }

    @Override
    public boolean isDcInRegion(String dc, String zone) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return xpipeMetaManager.getDcZone(dc).equalsIgnoreCase(zone);
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
    public String getDcByIpAndPeerClusterShard(String hostIp, Pair<String, String> peerClusterShard) {
        if (getAllKeeperContainersDcMap().containsKey(hostIp)) {
            return getAllKeeperContainersDcMap().get(hostIp);
        }

        if (getAllKeeperContainersDcMap().containsKey(hostIp)) {
            return getAllKeeperContainersDcMap().get(hostIp);
        }

        return getDcByIpAndClusterShard(hostIp, peerClusterShard);
    }

    @Override
    public Map<String, String> getAllKeeperContainersDcMap(){
        XpipeMeta xpipeMeta = getXpipeMeta();
        if (allKeeperContainersDcMap == null) {
            synchronized (this) {
                if (allKeeperContainersDcMap == null) {
                    Map<String, String> tempKeeperContainersDcMap = Maps.newLinkedHashMapWithExpectedSize(allKeeperSize);
                    xpipeMeta.getDcs().forEach((dcName, dcMeta)->{
                        dcMeta.getKeeperContainers().forEach(keeperContainerMeta -> {
                            tempKeeperContainersDcMap.put(keeperContainerMeta.getIp(), dcName);
                        });
                    });
                    allKeeperContainersDcMap = tempKeeperContainersDcMap;
                }
            }
        }
        return allKeeperContainersDcMap;
    }

    @Override
    public Map<String, String> getAllApplierContainersDcMap(){
        XpipeMeta xpipeMeta = getXpipeMeta();
        if (allApplierContainersDcMap == null) {
            synchronized (this) {
                if (allApplierContainersDcMap == null) {
                    Map<String, String> tempApplierContainersDcMap = Maps.newLinkedHashMapWithExpectedSize(allKeeperSize);
                    xpipeMeta.getDcs().forEach((dcName, dcMeta)->{
                        dcMeta.getApplierContainers().forEach(applierContainerMeta -> {
                            tempApplierContainersDcMap.put(applierContainerMeta.getIp(), dcName);
                        });
                    });
                    allApplierContainersDcMap = tempApplierContainersDcMap;
                }
            }
        }
        return allApplierContainersDcMap;
    }

    @Override
    public String getSentinelMonitorName(String clusterId, String shardId) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        String activeDc = xpipeMetaManager.getActiveDc(clusterId);
        return xpipeMetaManager.getSentinelMonitorName(activeDc, clusterId, shardId);
    }

    @Override
    public Set<HostPort> getActiveDcSentinels(String clusterId, String shardId) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        String activeDc = xpipeMetaManager.getActiveDc(clusterId);
        SentinelMeta sentinel = xpipeMetaManager.getSentinel(activeDc, clusterId, shardId);

        return new HashSet<>(IpUtils.parseAsHostPorts(sentinel.getAddress()));
    }

    @Override
    public Set<HostPort> getAllSentinels() {
        Set<HostPort> sentinels = new HashSet<>();
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        xpipeMetaManager.getAllSentinels().forEach(sentinelMeta -> sentinels.addAll(IpUtils.parseAsHostPorts(sentinelMeta.getAddress())));
        return sentinels;
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
    public List<RedisMeta> getRedisOfDcClusterShard(String dc, String cluster, String shard) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
        if (null == shardMeta) return Collections.emptyList();
        return shardMeta.getRedises();
    }

    @Override
    public List<KeeperMeta> getKeeperOfDcClusterShard(String dc, String cluster, String shard) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
        if (null == shardMeta) return Collections.emptyList();
        return shardMeta.getKeepers();
    }

    @Override
    public List<RedisMeta> getSlavesOfDcClusterShard(String dc, String cluster, String shard) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
        if (null == shardMeta) return Collections.emptyList();
        return shardMeta.getRedises().stream().filter(redisMeta -> !redisMeta.isMaster()).collect(Collectors.toList());
    }

    @Override
    public List<RedisMeta> getSlavesOfShard(String cluster, String shard) {
        List<RedisMeta> slaves = new ArrayList<>();
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        xpipeMetaManager.doGetDcs().forEach(dc -> {
            ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
            if (shardMeta != null) {
                shardMeta.getRedises().forEach(redisMeta -> {
                    if (!redisMeta.isMaster())
                        slaves.add(redisMeta);
                });
            }
        });
        return slaves;
    }

    @Override
    public List<RedisMeta> getAllInstancesOfShard(String cluster, String shard) {
        List<RedisMeta> instances = new ArrayList<>();
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        xpipeMetaManager.doGetDcs().forEach(dc -> {
            ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
            if (shardMeta != null) {
                instances.addAll(shardMeta.getRedises());
            }
        });
        return instances;
    }

    @Override
    public List<RedisMeta> getAllInstanceOfDc(String cluster, String dc) {
        return meta.getValue().getRedises(dc, cluster);
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
    public String getDcByIpAndClusterShard(String hostIp, Pair<String, String> clusterShard) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        Set<String> relatedDcs = xpipeMetaManager.getRelatedDcs(clusterShard.getKey(), clusterShard.getValue());

        for (String dc : relatedDcs) {
            for (RedisMeta redis : xpipeMetaManager.getRedises(dc, clusterShard.getKey(), clusterShard.getValue())) {
                if (redis.getIp().equalsIgnoreCase(hostIp)) {
                   return dc;
                }
            }
        }
        return null;
    }

    @Override
    public Triple<String, String, Long> findClusterShardBySentinelMonitor(String monitor) {
        if(StringUtil.isEmpty(monitor)) {
            return null;
        }

        Triple<String, String, Long> clusterShard = monitor2ClusterShard.get(monitor);
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
                    ClusterType azGroupType = StringUtil.isEmpty(clusterMeta.getAzGroupType())
                        ? null : ClusterType.lookup(clusterMeta.getAzGroupType());
                    if (clusterType.supportSingleActiveDC() && azGroupType != ClusterType.SINGLE_DC
                        && !clusterMeta.getActiveDc().equals(dcMeta.getId())) {
                        continue;
                    }

                    for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                        monitor2ClusterShard.put(shardMeta.getSentinelMonitorName(),
                                new Triple(clusterMeta.getId(), shardMeta.getId(), shardMeta.getSentinelId()));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("[loadSentinelMonitorInfo]", e);
            throw e;
        }
    }

    @Override
    public String getActiveDc(String clusterId){
        XpipeMetaManager xpipeMetaManager  =  meta.getValue();
        return xpipeMetaManager.getActiveDc(clusterId);
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

    @Override
    public boolean isAsymmetricCluster(String clusterName) {
        XpipeMeta xpipeMeta = meta.getKey();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            ClusterMeta clusterMeta = dcMeta.findCluster(clusterName);
            if (clusterMeta != null) {
                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                ClusterType azGroupType = StringUtil.isEmpty(clusterMeta.getAzGroupType())
                    ? null : ClusterType.lookup(clusterMeta.getAzGroupType());
                if (clusterType == ClusterType.ONE_WAY && azGroupType == ClusterType.SINGLE_DC) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Map<Long, String> dcShardIds(String clusterId, String dcId) {
        DcMeta dcMeta = meta.getKey().findDc(dcId);
        if (dcMeta == null) return new HashMap<>();
        ClusterMeta clusterMeta = dcMeta.findCluster(clusterId);
        if (clusterMeta == null) return new HashMap<>();
        return clusterMeta.getShards().values().stream().collect(Collectors.toMap(ShardMeta::getDbId, ShardMeta::getId));
    }

    protected void checkMeta(XpipeMeta future, int maxRemovedDcs, int maxRemovedClusterPercent) {
        if (meta == null) return;
        if (future == null) throw new LoadConsoleMetaException("xpipe meta from console is null");

        checkDcsCnt(meta.getKey(), future, maxRemovedDcs);

        for (DcMeta currentDcMeta : meta.getKey().getDcs().values()) {
            DcMeta futureDcMeta = future.findDc(currentDcMeta.getId());
            checkDcClustersCnt(currentDcMeta, futureDcMeta, maxRemovedClusterPercent);
        }
    }

    private void checkDcsCnt(XpipeMeta current, XpipeMeta future, int maxRemovedDcs) {
        int currentDcCount = current.getDcs().size();
        int futureDcCount = future.getDcs().size();
        if (currentDcCount - futureDcCount > maxRemovedDcs)
            throw new TooManyDcsRemovedException(String.format("current dcs:%s,future dcs:%s", current.getDcs().keySet(), future.getDcs().keySet()));
    }

    private void checkDcClustersCnt(DcMeta currentDcMeta, DcMeta futureDcMeta, int maxRemovedClusterPercent) {
        if (futureDcMeta == null) return;

        int currentClusterCount = currentDcMeta.getClusters().size();
        int futureClusterCount = futureDcMeta.getClusters().size();
        if ((currentClusterCount - futureClusterCount) > currentClusterCount * maxRemovedClusterPercent / 100)
            throw new TooManyClustersRemovedException(String.format("dc:%s, current cluster count:%d,future cluster count:%d", currentDcMeta.getId(), currentClusterCount, futureClusterCount));
    }

    @Override
    public Set<String> getAllShardNamesByClusterName(String clusterName) {
        Set<String> shards = new HashSet<>();
        XpipeMeta xpipeMeta = meta.getKey();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            ClusterMeta clusterMeta = dcMeta.findCluster(clusterName);
            if (clusterMeta != null) {
               Map<String, ShardMeta> shardMetaMap = clusterMeta.getShards();
               shards.addAll(shardMetaMap.keySet());
            }
        }
        return shards;
    }

    @Override
    public Map<String, Integer> getClusterCntMap(String clusterName) {
        Map<String, Integer> clusterCntMap = new HashMap<>();
        for (DcMeta dcMeta : meta.getKey().getDcs().values()) {
            ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterName);
            if(clusterMeta == null) {
                continue;
            }
            int cnt = 0;
            for(ShardMeta shardMeta : clusterMeta.getShards().values()) {
                cnt += shardMeta.getRedises().size();
            }
            clusterCntMap.put(dcMeta.getId(), cnt);
        }
        return clusterCntMap;
    }

    @Override
    public boolean isDcClusterMigratable(String clusterName, String dc) {
        if (StringUtil.isEmpty(clusterName) || StringUtil.isEmpty(dc)) return false;
        DcMeta dcMeta = meta.getKey().getDcs().get(dc);
        if (dcMeta == null) return false;
        ClusterMeta clusterMeta = dcMeta.findCluster(clusterName);
        if (clusterMeta == null) return false;
        ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
        if (clusterType != HETERO) {
            return clusterType.supportMigration();
        }
        return ClusterType.lookup(clusterMeta.getAzGroupType()).supportMigration();
    }

    @Override
    public boolean anyDcMigratable(String clusterName) {
        if (StringUtil.isEmpty(clusterName)) return false;
        for (DcMeta dcMeta : meta.getKey().getDcs().values()) {
            ClusterMeta clusterMeta = dcMeta.findCluster(clusterName);
            if (clusterMeta == null) continue;
            ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
            if ((clusterType != HETERO && clusterType.supportMigration())
                    || (!StringUtil.isEmpty(clusterMeta.getAzGroupType()) && ClusterType.lookup(clusterMeta.getAzGroupType()).supportMigration())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getMigratableClustersCountByActiveDc(String dc) {
        if (StringUtil.isEmpty(dc)) return 0;
        DcMeta dcMeta = meta.getKey().findDc(dc);
        if (null == dcMeta) return 0;

        int cnt = 0;
        for (ClusterMeta cluster: dcMeta.getClusters().values()) {
            if (!dc.equalsIgnoreCase(cluster.getActiveDc())) continue;
            ClusterType type = ClusterType.lookup(cluster.getType());
            if (type.equals(HETERO)) {
                type = ClusterType.lookup(cluster.getAzGroupType());
            }
            if (type.supportMigration()) cnt++;
        }
        return cnt;
    }

    @Override
    public Map<String, Integer> getAllDcMigratableClustersCnt() {
        Map<String, Integer> ret = new HashMap<>();
        for (String dc: meta.getKey().getDcs().keySet()) {
            ret.put(dc, getMigratableClustersCountByActiveDc(dc));
        }
        return ret;
    }

    @Override
    public boolean isBackupDcAndCrossRegion(String currentDc, String activeDc, List<String> dcs) {
        dcs = dcs.stream().map(String::toLowerCase).collect(Collectors.toList());
        return isCrossRegion(activeDc, currentDc) && dcs.contains(currentDc.toLowerCase());
    }

    @VisibleForTesting
    public AbstractMetaCache setMeta(Pair<XpipeMeta, XpipeMetaManager> meta) {
        this.meta = meta;
        return this;
    }

    @VisibleForTesting
    public AbstractMetaCache setMonitor2ClusterShard(Map<String, Triple<String, String, Long>> monitorMap) {
        this.monitor2ClusterShard = monitorMap;
        return this;
    }

}
