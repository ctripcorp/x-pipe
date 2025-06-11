package com.ctrip.xpipe.redis.console.service.vo;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.cluster.Hints;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * @author chen.zhu
 * <p>
 * Apr 03, 2018
 */
public class DcMetaBuilder extends AbstractCommand<Map<String, DcMeta>> {

    private Map<String, DcMeta> dcMetaMap;

    private List<DcTbl> allDcsTblList;

    private Map<Long, String> dcNameMap;

    private RedisMetaService redisMetaService;

    private DcClusterService dcClusterService;

    private ClusterMetaService clusterMetaService;

    private DcClusterShardService dcClusterShardService;

    private AzGroupClusterRepository azGroupClusterRepository;

    private AzGroupCache azGroupCache;

    private DcService dcService;

    private ExecutorService executors;

    private RetryCommandFactory factory;

    private ConsoleConfig consoleConfig;

    private Set<String> interestClusterTypes;

    @VisibleForTesting
    protected Map<Long, List<DcClusterTbl>> cluster2DcClusterMap;

    protected Map<Long, List<AzGroupClusterEntity>> cluster2AzGroupClusterMap;

    private Map<Long, List<DcClusterShardTbl>> dcCluster2DcClusterShardMap;

    private Map<Long, List<DcClusterShardTbl>> dc2DcClusterShardMap;

    private static final String DC_NAME_DELIMITER = ",";

    public DcMetaBuilder(Map<String, DcMeta> dcMetaMap, List<DcTbl> allDcsList, Set<String> clusterTypes, ExecutorService executors, RedisMetaService redisMetaService, DcClusterService dcClusterService,
                         ClusterMetaService clusterMetaService, DcClusterShardService dcClusterShardService, DcService dcService, AzGroupClusterRepository azGroupClusterRepository, AzGroupCache azGroupCache,
                         RetryCommandFactory factory, ConsoleConfig consoleConfig) {
        this.dcMetaMap = dcMetaMap;
        this.allDcsTblList = allDcsList;
        this.interestClusterTypes = clusterTypes;
        this.executors = executors;
        this.redisMetaService = redisMetaService;
        this.dcClusterService = dcClusterService;
        this.clusterMetaService = clusterMetaService;
        this.dcClusterShardService = dcClusterShardService;
        this.azGroupClusterRepository = azGroupClusterRepository;
        this.azGroupCache = azGroupCache;
        this.dcService = dcService;
        this.factory = factory;
        this.consoleConfig = consoleConfig;
    }

    @Override
    protected void doExecute() throws Exception {
        getLogger().debug("[doExecute] start build DcMeta");
        SequenceCommandChain sequenceCommandChain = new SequenceCommandChain(false);

        ParallelCommandChain parallelCommandChain = new ParallelCommandChain(executors, false);
        parallelCommandChain.add(retry3TimesUntilSuccess(new DcCluster2dcClusterShardMapCommand()));
        parallelCommandChain.add(retry3TimesUntilSuccess(new Cluster2DcClusterMapCommand()));
        parallelCommandChain.add(retry3TimesUntilSuccess(new Cluster2AzGroupClusterMapCommand()));
        parallelCommandChain.add(retry3TimesUntilSuccess(new GetDcIdNameMapCommand()));

        sequenceCommandChain.add(parallelCommandChain);
        sequenceCommandChain.add(retry3TimesUntilSuccess(new BuildDcMetaCommand()));

        getLogger().debug("[doExecute] commands: {}", sequenceCommandChain);

        sequenceCommandChain.future().addListener(commandFuture -> {
            getLogger().debug("[doExecute] end build DcMeta");
            if(commandFuture.isSuccess()) {
                future().setSuccess(dcMetaMap);
            } else {
                future().setFailure(commandFuture.cause());
            }
        });
        sequenceCommandChain.execute(executors);
    }

    @Override
    protected void doReset() {
        // remove all clusters if failed
        for (DcMeta dcMeta : dcMetaMap.values()) {
            dcMeta.getClusters().clear();
        }
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    private  <T> Command<T> retry3TimesUntilSuccess(Command<T> command) {
        return factory.createRetryCommand(command);
    }

    @VisibleForTesting
    public ClusterMeta getOrCreateClusterMeta(DcMeta dcMeta, Long dcId, ClusterTbl cluster,
        DcClusterTbl dcClusterInfo, AzGroupClusterEntity azGroupCluster) {
        return MapUtils.getOrCreate(dcMeta.getClusters(), cluster.getClusterName(), new ObjectFactory<ClusterMeta>(){
            @Override
            public ClusterMeta create() {
                ClusterMeta clusterMeta = new ClusterMeta(cluster.getClusterName());
                clusterMeta.setDbId(cluster.getId());
                clusterMeta.setParent(dcMeta);
                clusterMeta.setOrgId(Math.toIntExact(cluster.getClusterOrgId()));
                clusterMeta.setAdminEmails(cluster.getClusterAdminEmails());
                clusterMeta.setType(cluster.getClusterType());
                clusterMeta.setActiveRedisCheckRules(dcClusterInfo == null ? null : dcClusterInfo.getActiveRedisCheckRules());
                clusterMeta.setClusterDesignatedRouteIds(cluster.getClusterDesignatedRouteIds());
                clusterMeta.setDownstreamDcs("");

                // TODO:下一版本删除DcGroup相关逻辑
                clusterMeta.setDcGroupName(getDcGroupName(dcMeta, dcClusterInfo));
                if (ClusterType.ONE_WAY.name().equalsIgnoreCase(cluster.getClusterType())) {
                    if (dcClusterInfo == null || dcClusterInfo.getGroupType() == null) {
                        clusterMeta.setDcGroupType(DcGroupType.DR_MASTER.toString());
                    } else {
                        clusterMeta.setDcGroupType(dcClusterInfo.getGroupType());
                    }
                }

                if (ClusterType.lookup(clusterMeta.getType()).supportMultiActiveDC()) {
                    clusterMeta.setDcs(getDcs(cluster));
                } else {
                    DcTbl proto = new DcTbl().setId(dcId);
                    long activeDcId = clusterMetaService.getClusterMetaCurrentPrimaryDc(proto, cluster);
                    clusterMeta.setActiveDc(dcNameMap.get(activeDcId));
                    clusterMeta.setBackupDcs(getBackupDcs(cluster, activeDcId));
                }

                if (azGroupCluster != null) {
                    ClusterType azGroupType = ClusterType.lookup(azGroupCluster.getAzGroupClusterType());
                    AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
                    clusterMeta.setAzGroupName(azGroup.getName());
                    clusterMeta.setAzGroupType(azGroupType.toString());

                    ClusterType clusterType = ClusterType.lookup(cluster.getClusterType());
                    if (clusterType == ClusterType.HETERO && azGroupType == ClusterType.SINGLE_DC) {
                        String activeAz = dcNameMap.get(azGroupCluster.getActiveAzId());
                        clusterMeta.setActiveDc(activeAz);
                        List<String> azs = azGroup.getAzsAsList();
                        azs.remove(activeAz);
                        clusterMeta.setBackupDcs(String.join(DC_NAME_DELIMITER, azs));
                    }
                }

                return clusterMeta;
            }
        });
    }

    @VisibleForTesting
    protected String getBackupDcs(ClusterTbl cluster, long activeDcId) {
        List<DcClusterTbl> relatedDcClusters = this.cluster2DcClusterMap.get(cluster.getId());
        if (CollectionUtils.isEmpty(relatedDcClusters)) {
            return "";
        }
        List<AzGroupClusterEntity> azGroupClusters =
            cluster2AzGroupClusterMap.getOrDefault(cluster.getId(), Collections.emptyList());
        Set<Long> singleDcAzGroupClusterIds = azGroupClusters.stream()
            .filter(agc -> ClusterType.isSameClusterType(agc.getAzGroupClusterType(), ClusterType.SINGLE_DC))
            .map(AzGroupClusterEntity::getId)
            .collect(Collectors.toSet());

        List<String> backupDcs = relatedDcClusters.stream()
            .filter(dcClusterTbl -> dcClusterTbl.getDcId() != activeDcId
                && !singleDcAzGroupClusterIds.contains(dcClusterTbl.getAzGroupClusterId()))
            .map(dcClusterTbl -> dcNameMap.get(dcClusterTbl.getDcId()))
            .collect(Collectors.toList());
        return String.join(DC_NAME_DELIMITER, backupDcs);
    }

    protected String getDcs(ClusterTbl cluster) {
        List<String> allDcs = new ArrayList<>();
        List<DcClusterTbl> relatedDcClusters = this.cluster2DcClusterMap.get(cluster.getId());
        if (relatedDcClusters == null) {
            return null;
        }

        relatedDcClusters.forEach(dcClusterTbl ->
            allDcs.add(dcNameMap.get(dcClusterTbl.getDcId()))
        );

        if (!allDcs.isEmpty()) return String.join(DC_NAME_DELIMITER, allDcs);
        return null;
    }

    public ShardMeta getOrCreateShardMeta(DcMeta dcMeta, String clusterId, ShardTbl shard, long sentinelId) {
        ClusterMeta clusterMeta = dcMeta.findCluster(clusterId);
        if(clusterMeta == null) {
            throw new IllegalArgumentException(String.format("Cluster: %s not found in dcMeta", clusterId));
        }
        return MapUtils.getOrCreate(clusterMeta.getShards(), shard.getShardName(), new ObjectFactory<ShardMeta>() {
            @Override
            public ShardMeta create() {
                ShardMeta shardMeta = new ShardMeta(shard.getShardName());
                shardMeta.setDbId(shard.getId());
                shardMeta.setParent(clusterMeta);
                shardMeta.setSentinelMonitorName(SentinelUtil.getSentinelMonitorName(clusterId, shard.getSetinelMonitorName(), ClusterType.lookup(clusterMeta.getType()).equals(ClusterType.CROSS_DC) ? consoleConfig.crossDcSentinelMonitorNameSuffix() : dcMeta.getId()));
                shardMeta.setSentinelId(sentinelId);
                return shardMeta;
            }
        });
    }

    class DcCluster2dcClusterShardMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                dcCluster2DcClusterShardMap = Maps.newHashMap();
                dc2DcClusterShardMap = Maps.newHashMap();

                List<DcClusterShardTbl> allDcClusterShards = new LinkedList<>();
                ParallelCommandChain chain = new ParallelCommandChain(executors, false);

                for (DcTbl dcTbl : allDcsTblList) {
                    chain.add(new AbstractCommand<Object>() {
                        @Override
                        public String getName() {
                            return "FindAllByDcIdAndInClusterTypes";
                        }

                        @Override
                        protected void doExecute() throws Throwable {
                            List<DcClusterShardTbl> res = dcClusterShardService.findAllByDcIdAndInClusterTypes(dcTbl.getId(), interestClusterTypes);
                            synchronized (allDcClusterShards) {
                                allDcClusterShards.addAll(res);
                            }
                            future().setSuccess();
                        }

                        @Override
                        protected void doReset() {

                        }
                    });
                }

                chain.execute().get();

                for (DcClusterShardTbl dcClusterShardTbl : allDcClusterShards) {
                    if (dcClusterShardTbl.getDcClusterInfo() == null) {
                        getLogger().warn("dcClusterInfo in dcClusterShard null");
                        continue;
                    }
                    List<DcClusterShardTbl> dcClusterShardTbls = MapUtils.getOrCreate(dcCluster2DcClusterShardMap,
                            dcClusterShardTbl.getDcClusterInfo().getDcClusterId(), LinkedList::new);
                    dcClusterShardTbls.add(dcClusterShardTbl);

                    List<DcClusterShardTbl> dc2ClusterShardTbls = MapUtils.getOrCreate(dc2DcClusterShardMap,
                            dcClusterShardTbl.getDcClusterInfo().getDcId(), LinkedList::new);
                    dc2ClusterShardTbls.add(dcClusterShardTbl);
                }

                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            dcCluster2DcClusterShardMap = null;
        }

        @Override
        public String getName() {
            return DcCluster2dcClusterShardMapCommand.class.getSimpleName();
        }
    }

    class Cluster2DcClusterMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                cluster2DcClusterMap = Maps.newHashMap();
                List<DcClusterTbl> allDcClusters = dcClusterService.findAllDcClusters();

                for (DcClusterTbl dcClusterTbl : allDcClusters) {
                    List<DcClusterTbl> dcClusters = MapUtils.getOrCreate(cluster2DcClusterMap,
                        dcClusterTbl.getClusterId(), LinkedList::new);
                    dcClusters.add(dcClusterTbl);
                }
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            cluster2DcClusterMap = null;
        }

        @Override
        public String getName() {
            return Cluster2DcClusterMapCommand.class.getSimpleName();
        }
    }

    class Cluster2AzGroupClusterMapCommand extends AbstractCommand<Void> {
        @Override
        protected void doExecute() throws Throwable {
            try {
                cluster2AzGroupClusterMap = Maps.newHashMap();
                List<AzGroupClusterEntity> allAzGroupClusters = azGroupClusterRepository.selectAll();

                for (AzGroupClusterEntity azGroupCluster : allAzGroupClusters) {
                    List<AzGroupClusterEntity> azGroupClusters = MapUtils.getOrCreate(cluster2AzGroupClusterMap,
                        azGroupCluster.getClusterId(), LinkedList::new);
                    azGroupClusters.add(azGroupCluster);
                }
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            cluster2AzGroupClusterMap = null;
        }

        @Override
        public String getName() {
            return Cluster2AzGroupClusterMapCommand.class.getSimpleName();
        }
    }

    class GetDcIdNameMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                dcNameMap = dcService.dcNameMap();
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            dcNameMap = null;
        }

        @Override
        public String getName() {
            return GetDcIdNameMapCommand.class.getSimpleName();
        }
    }

    class BuildDcMetaCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                for (Map.Entry<Long, List<DcClusterShardTbl>> entry : dc2DcClusterShardMap.entrySet()) {
                    Long dcId = entry.getKey();
                    DcMeta dcMeta = dcMetaMap.get(dcNameMap.get(dcId).toUpperCase());
                    if (dcMeta == null) {
                        continue;
                    }
                    for (DcClusterShardTbl dcClusterShard : entry.getValue()) {
                        ClusterTbl cluster = dcClusterShard.getClusterInfo();
                        ClusterMeta clusterMeta = getOrCreateClusterMeta(dcMeta, dcId, cluster,
                            getDcClusterInfo(cluster.getId(), dcId), getAzGroupCluster(cluster.getId(), dcId));
                        ShardMeta shardMeta = getOrCreateShardMeta(dcMeta, clusterMeta.getId(),
                            dcClusterShard.getShardInfo(), dcClusterShard.getSetinelId());

                        RedisTbl redis = dcClusterShard.getRedisInfo();
                        if (Server.SERVER_ROLE.KEEPER.sameRole(redis.getRedisRole())) {
                            shardMeta.addKeeper(redisMetaService.getKeeperMeta(shardMeta, redis));
                        } else {
                            if (!StringUtil.isEmpty(clusterMeta.getActiveDc())
                                    && !clusterMeta.getActiveDc().equalsIgnoreCase(dcMeta.getId())) {
                                // redis role haven't changed but activeDc already switch in migration
                                redis.setMaster(false);
                            }
                            shardMeta.addRedis(redisMetaService.getRedisMeta(shardMeta, redis));
                        }
                    }
                }

                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        private boolean clusterHasMasterDc(ClusterMeta clusterMeta) {
            List<AzGroupClusterEntity> azGroupClusters = cluster2AzGroupClusterMap.get(clusterMeta.getDbId());
            if (CollectionUtils.isEmpty(azGroupClusters)) {
                return false;
            }
            for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
                String azGroupType = azGroupCluster.getAzGroupClusterType();
                if (ClusterType.isSameClusterType(azGroupType, ClusterType.SINGLE_DC)) {
                    return true;
                }
            }
            return false;
        }

        @VisibleForTesting
        protected DcClusterTbl getDcClusterInfo(long clusterId, long dcId) {

            List<DcClusterTbl> dcClusterTblList = cluster2DcClusterMap.get(clusterId);

            if (dcClusterTblList == null) {
                getLogger().warn("[getDcClusterInfo] dcCluster not found, clusterId={}", clusterId);
                return null;
            }

            for(DcClusterTbl dcClusterTbl: dcClusterTblList) {
                if (dcClusterTbl.getDcId() == dcId) {
                    return dcClusterTbl;
                }
            }
            return null;
        }

        protected AzGroupClusterEntity getAzGroupCluster(long clusterId, long dcId) {
            List<AzGroupClusterEntity> azGroupClusters = cluster2AzGroupClusterMap.get(clusterId);
            if (CollectionUtils.isEmpty(azGroupClusters)) {
                getLogger().debug("[getAzGroupCluster] azGroupCluster not found, clusterId={}", clusterId);
                return null;
            }

            String dc = dcNameMap.get(dcId);
            for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
                AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
                if (azGroup.containsAz(dc)) {
                    return azGroupCluster;
                }
            }

            return null;
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return BuildDcMetaCommand.class.getSimpleName();
        }
    }

    @VisibleForTesting
    BuildDcMetaCommand createBuildDcMetaCommand() {
        return new BuildDcMetaCommand();
    }

    private String getDcGroupName(DcMeta dcMeta, DcClusterTbl dcClusterInfo) {
        if (dcClusterInfo == null || dcClusterInfo.getGroupName() == null) {
            return dcMeta.getId();
        }

        return dcClusterInfo.getGroupName();
    }

    /**------------------Visible for Test-----------------------*/
    public DcMetaBuilder setDcNameMap(Map<Long, String> dcNameMap) {
        this.dcNameMap = dcNameMap;
        return this;
    }

    public DcMetaBuilder setCluster2DcClusterMap(Map<Long, List<DcClusterTbl>> cluster2DcClusterMap) {
        this.cluster2DcClusterMap = cluster2DcClusterMap;
        return this;
    }

}
