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
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

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

    private Map<String, Long> dcNameZoneMap;

    private Map<Long, String> zoneNameMap;

    private Map<Long, Long> keeperContainerIdDcMap;

    private Map<Long, List<ApplierTbl>> replId2AppliersMap;

    private Set<Long> shardIdWithAppliers;

    private List<ReplDirectionTbl> replDirectionTblList;

    private RedisMetaService redisMetaService;

    private DcClusterService dcClusterService;

    private ClusterMetaService clusterMetaService;

    private DcClusterShardService dcClusterShardService;

    private ReplDirectionService replDirectionService;

    private ZoneService zoneService;

    private KeeperContainerService keeperContainerService;

    private ApplierService applierService;

    private DcService dcService;

    private ClusterService clusterService;

    private ShardService shardService;

    private RedisService redisService;

    private ExecutorService executors;

    private RetryCommandFactory factory;

    private ConsoleConfig consoleConfig;

    private Set<String> interestClusterTypes;

    @VisibleForTesting
    protected Map<Long, List<DcClusterTbl>> cluster2DcClusterMap;

    private Map<Long, List<DcClusterShardTbl>> dcCluster2DcClusterShardMap;

    private Map<Long, List<DcClusterShardTbl>> dc2DcClusterShardMap;

    private static final String DC_NAME_DELIMITER = ",";

    public DcMetaBuilder(Map<String, DcMeta> dcMetaMap, List<DcTbl> allDcsList, Set<String> clusterTypes, ExecutorService executors, RedisMetaService redisMetaService, DcClusterService dcClusterService,
                         ClusterMetaService clusterMetaService, DcClusterShardService dcClusterShardService, DcService dcService, ClusterService clusterService, ShardService shardService, RedisService redisService,
                         ReplDirectionService replDirectionService, ZoneService zoneService, KeeperContainerService keeperContainerService, ApplierService applierService,
                         RetryCommandFactory factory, ConsoleConfig consoleConfig) {
        this.dcMetaMap = dcMetaMap;
        this.allDcsTblList = allDcsList;
        this.interestClusterTypes = clusterTypes;
        this.executors = executors;
        this.redisMetaService = redisMetaService;
        this.dcClusterService = dcClusterService;
        this.clusterMetaService = clusterMetaService;
        this.dcClusterShardService = dcClusterShardService;
        this.dcService = dcService;
        this.clusterService = clusterService;
        this.shardService = shardService;
        this.redisService = redisService;
        this.replDirectionService = replDirectionService;
        this.zoneService = zoneService;
        this.keeperContainerService = keeperContainerService;
        this.applierService = applierService;
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
        parallelCommandChain.add(retry3TimesUntilSuccess(new GetDcIdNameMapCommand()));

        parallelCommandChain.add(retry3TimesUntilSuccess(new GetReplDirectionListCommand()));
        parallelCommandChain.add(retry3TimesUntilSuccess(new GetReplId2ApplierMapCommand()));
        parallelCommandChain.add(retry3TimesUntilSuccess(new GetDcNameToZoneIdMapCommand()));
        parallelCommandChain.add(retry3TimesUntilSuccess(new GetZoneIdNameMapCommand()));
        parallelCommandChain.add(retry3TimesUntilSuccess(new GetKeeperContainerIdDcMapCommand()));

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
    public ClusterMeta getOrCreateClusterMeta(DcMeta dcMeta, Long dcId, ClusterTbl cluster, DcClusterTbl dcClusterInfo) {
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

                return clusterMeta;
            }
        });
    }

    @VisibleForTesting
    protected String getBackupDcs(ClusterTbl cluster, long activeDcId) {
        List<DcClusterTbl> relatedDcClusters = this.cluster2DcClusterMap.get(cluster.getId());
        if (relatedDcClusters == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        relatedDcClusters.forEach(dcClusterTbl -> {
            if(dcClusterTbl.getDcId() != activeDcId && DcGroupType.isNullOrDrMaster(dcClusterTbl.getGroupType())) {
                sb.append(dcNameMap.get(dcClusterTbl.getDcId())).append(",");
            }
        });
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
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

    public ShardMeta getOrCreateShardMeta(SourceMeta sourceMeta, ShardTbl shard) {
        return MapUtils.getOrCreate(sourceMeta.getShards(), shard.getShardName(), () -> {
            ShardMeta shardMeta = new ShardMeta(shard.getShardName());
            shardMeta.setDbId(shard.getId());
            shardMeta.setParent(sourceMeta);
            return shardMeta;
        });
    }

    class DcCluster2dcClusterShardMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                dcCluster2DcClusterShardMap = Maps.newHashMap();
                dc2DcClusterShardMap = Maps.newHashMap();

                List<DcClusterShardTbl> allDcClusterShards = findAllByDcIdAndInClusterTypes();
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

        private List<DcClusterShardTbl> findAllByDcIdAndInClusterTypes() {

            List<ClusterTbl> allClusterTbls = clusterService.findAllClustersWithOrgInfo();
            List<DcClusterTbl> allDcClusterTbls = dcClusterService.findAllDcClusters();
            List<DcClusterShardTbl> allDcClusterShardTbls = dcClusterShardService.findAll();
            List<ShardTbl> allShardTbls = shardService.findAll();
            List<RedisTbl> allRedisTbls = redisService.findByRole(XPipeConsoleConstant.ROLE_REDIS);
            allRedisTbls.addAll(redisService.findByRole(XPipeConsoleConstant.ROLE_KEEPER));

            Map<Long, ClusterTbl> clusterId2ClusterTbl = allClusterTbls.stream().filter(clusterTbl -> interestClusterTypes.contains(clusterTbl.getClusterType().toUpperCase())).collect(Collectors.toMap(ClusterTbl::getId, clusterTbl -> clusterTbl));
            Map<Long, List<DcClusterTbl>> dcId2DcClusterTbls = new HashMap<>();
            for (DcClusterTbl dcClusterTbl : allDcClusterTbls) {
                List<DcClusterTbl> dcClusterTbls = MapUtils.getOrCreate(dcId2DcClusterTbls, dcClusterTbl.getDcId(), LinkedList::new);
                dcClusterTbls.add(dcClusterTbl);
            }

            Map<Long, List<DcClusterShardTbl>> dcClusterId2DcClusterShardTbls = new HashMap<>();
            for (DcClusterShardTbl dcClusterShardTbl : allDcClusterShardTbls) {
                List<DcClusterShardTbl> dcClusterShardTbls = MapUtils.getOrCreate(dcClusterId2DcClusterShardTbls, dcClusterShardTbl.getDcClusterId(), LinkedList::new);
                dcClusterShardTbls.add(dcClusterShardTbl);
            }

            Map<Long, ShardTbl> shardTblMap = allShardTbls.stream().collect(Collectors.toMap(ShardTbl::getId, shardTbl -> shardTbl));
            Map<Long, List<RedisTbl>> dcClusterShardId2RedisTbls = new HashMap<>();
            for (RedisTbl redisTbl : allRedisTbls) {
                List<RedisTbl> redisTbls = MapUtils.getOrCreate(dcClusterShardId2RedisTbls, redisTbl.getDcClusterShardId(), LinkedList::new);
                redisTbls.add(redisTbl);
            }

            return buildDcClusterShardTbls(allDcsTblList, clusterId2ClusterTbl, dcId2DcClusterTbls, dcClusterId2DcClusterShardTbls, shardTblMap, dcClusterShardId2RedisTbls);
        }

        List<DcClusterShardTbl> buildDcClusterShardTbls(List<DcTbl> allDcsTblList, Map<Long, ClusterTbl> clusterId2ClusterTbl,
                                                        Map<Long, List<DcClusterTbl>> dcId2DcClusterTbls, Map<Long, List<DcClusterShardTbl>> dcClusterId2DcClusterShardTbls,
                                                        Map<Long, ShardTbl> shardTblMap, Map<Long, List<RedisTbl>> dcClusterShardId2RedisTbls) {
            List<DcClusterShardTbl> dcClusterShardTbls = new LinkedList<>();
            for (DcTbl dcTbl : allDcsTblList) {

                List<DcClusterTbl> dcClusterTbls = dcId2DcClusterTbls.get(dcTbl.getId());
                if (dcClusterTbls == null)
                    continue;

                for (DcClusterTbl dcClusterTbl : dcClusterTbls) {
                    List<DcClusterShardTbl> dcClusterShardTbls1 = dcClusterId2DcClusterShardTbls.get(dcClusterTbl.getDcClusterId());
                    if (dcClusterShardTbls1 == null)
                        continue;

                    ClusterTbl clusterTbl = clusterId2ClusterTbl.get(dcClusterTbl.getClusterId());
                    if (clusterTbl == null)
                        continue;

                    List<DcClusterShardTbl> dcClusterShardTblsWithRedis = new LinkedList<>();

                    for (DcClusterShardTbl dcClusterShardTbl : dcClusterShardTbls1) {
                        ShardTbl shardTbl = shardTblMap.get(dcClusterShardTbl.getShardId());
                        if (shardTbl == null)
                            continue;

                        List<RedisTbl> redisTbls = dcClusterShardId2RedisTbls.get(dcClusterShardTbl.getDcClusterShardId());
                        if (redisTbls == null)
                            continue;

                        redisTbls.forEach(redisTbl -> {
                            DcClusterShardTbl local = new DcClusterShardTbl().
                                    setDcClusterShardId(dcClusterShardTbl.getDcClusterShardId()).
                                    setDcClusterId(dcClusterShardTbl.getDcClusterId()).
                                    setShardId(dcClusterShardTbl.getShardId()).
                                    setSetinelId(dcClusterShardTbl.getSetinelId());
                            local.setDcClusterInfo(dcClusterTbl);
                            local.setClusterInfo(clusterTbl);
                            local.setShardInfo(shardTbl);
                            local.setRedisInfo(redisTbl);
                            dcClusterShardTblsWithRedis.add(local);
                        });
                    }

                    dcClusterShardTbls.addAll(dcClusterShardTblsWithRedis);
                }
            }
            return dcClusterShardTbls;
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
                            dcClusterTbl.getClusterId(), new ObjectFactory<List<DcClusterTbl>>() {
                                @Override
                                public List<DcClusterTbl> create() {
                                    return new LinkedList<>();
                                }
                            });
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

    class GetDcNameToZoneIdMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                dcNameZoneMap = dcService.dcNameZoneMap();
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            dcNameZoneMap = null;
        }

        @Override
        public String getName() {
            return GetDcNameToZoneIdMapCommand.class.getSimpleName();
        }
    }

    class GetZoneIdNameMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                zoneNameMap = zoneService.zoneNameMap();
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            zoneNameMap = null;
        }

        @Override
        public String getName() {
            return GetZoneIdNameMapCommand.class.getSimpleName();
        }
    }

    class GetReplDirectionListCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                replDirectionTblList = replDirectionService.findAllReplDirectionJoinClusterTbl();
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            replDirectionTblList = null;
        }

        @Override
        public String getName() {
            return GetReplDirectionListCommand.class.getSimpleName();
        }
    }

    class GetReplId2ApplierMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                List<ApplierTbl> applierTblList = applierService.findAll();
                replId2AppliersMap = new HashMap<>();
                shardIdWithAppliers = new HashSet<>();
                for (ApplierTbl applierTbl : applierTblList) {
                    shardIdWithAppliers.add(applierTbl.getShardId());
                    List<ApplierTbl> applierTbls = MapUtils.getOrCreate(replId2AppliersMap, applierTbl.getReplDirectionId(), ArrayList::new);
                    applierTbls.add(applierTbl);
                }
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            replId2AppliersMap = null;
        }

        @Override
        public String getName() {
            return GetReplId2ApplierMapCommand.class.getSimpleName();
        }
    }

    class GetKeeperContainerIdDcMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                keeperContainerIdDcMap = Maps.newHashMap();
                List<KeepercontainerTbl> keepercontainerTblList = keeperContainerService.findAll();
                for (KeepercontainerTbl keepercontainerTbl : keepercontainerTblList) {
                    keeperContainerIdDcMap.put(keepercontainerTbl.getKeepercontainerId(), keepercontainerTbl.getKeepercontainerDc());
                }
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            keeperContainerIdDcMap = null;
        }

        @Override
        public String getName() {
            return GetKeeperContainerIdDcMapCommand.class.getSimpleName();
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
                        ClusterMeta clusterMeta = getOrCreateClusterMeta(dcMeta, dcId,
                                dcClusterShard.getClusterInfo(), getDcClusterInfo(dcClusterShard.getClusterInfo().getId(), dcId));
                        ShardMeta shardMeta = getOrCreateShardMeta(dcMeta, clusterMeta.getId(),
                                dcClusterShard.getShardInfo(), dcClusterShard.getSetinelId());

                        RedisTbl redis = dcClusterShard.getRedisInfo();
                        if (Server.SERVER_ROLE.KEEPER.sameRole(redis.getRedisRole())) {
                            if (dcId.equals(keeperContainerIdDcMap.get(redis.getKeepercontainerId()))) {
                                shardMeta.addKeeper(redisMetaService.getKeeperMeta(shardMeta, redis));
                            }
                        } else {
                            shardMeta.addRedis(redisMetaService.getRedisMeta(shardMeta, redis));
                        }
                    }
                    if (interestClusterTypes.contains(ClusterType.ONE_WAY.name())) {
                        buildHeteroMeta(dcMeta, dcId);
                    }
                }
                addClusterHints();

                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        private void addClusterHints() {
            for (DcMeta dcMeta: dcMetaMap.values()) {
                for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                    if (clusterHasApplier(clusterMeta)) {
                        clusterMeta.setHints(Hints.append(clusterMeta.getHints(), Hints.APPLIER_IN_CLUSTER));
                    }
                    if (clusterHasMasterDc(clusterMeta)) {
                        clusterMeta.setHints(Hints.append(clusterMeta.getHints(), Hints.MASTER_DC_IN_CLUSTER));
                    }
                }
            }
        }

        private boolean clusterHasApplier(ClusterMeta clusterMeta) {
            for (ShardMeta shardMeta : clusterMeta.getAllShards().values()) {
                if (shardIdWithAppliers.contains(shardMeta.getDbId())) {
                    return true;
                }
            }
            return false;
        }

        private boolean clusterHasMasterDc(ClusterMeta clusterMeta) {
            List<DcClusterTbl> dcClusterTblList = cluster2DcClusterMap.get(clusterMeta.getDbId());
            if (dcClusterTblList == null) {
                return false;
            }
            for (DcClusterTbl dcClusterTbl : dcClusterTblList) {
                if (DcGroupType.MASTER.name().equals(dcClusterTbl.getGroupType())) {
                    return true;
                }
            }
            return false;
        }

        private void buildHeteroMeta(DcMeta dcMeta, Long dcId) {

            List<ReplDirectionTbl> toCurrentDcOrFromCurrentDcList = replDirectionTblList
                    .stream()
                    .filter(a -> a.getFromDcId() == dcId || a.getToDcId() == dcId)
                    .collect(Collectors.toList());

            for (ReplDirectionTbl replDirection : toCurrentDcOrFromCurrentDcList) {
                long clusterId = replDirection.getClusterId();
                DcClusterTbl dcClusterTbl = getDcClusterInfo(clusterId, dcId);
                ClusterMeta clusterMeta = getOrCreateClusterMeta(dcMeta, dcId, replDirection.getClusterInfo(), dcClusterTbl);
                if (dcClusterTbl == null) {
                    getLogger().warn("[buildHeteroMeta] dcCluster not found; clusterId={}", clusterId);
                    continue;
                }
                if (replDirection.getToDcId() == dcId) {
                    if (DcGroupType.isSameGroupType(dcClusterTbl.getGroupType(), DcGroupType.MASTER)) {
                        SourceMeta sourceMeta = buildSourceMeta(clusterMeta, replDirection.getSrcDcId(), replDirection.getFromDcId());
                        buildSourceShardMetas(dcId, sourceMeta, clusterMeta.getId(), clusterId, replDirection.getSrcDcId());
                    }
                }
                if (replDirection.getFromDcId() == dcId) {
                    setDownstreamDcs(clusterMeta, replDirection);
                }
            }
        }

        private void buildSourceShardMetas(Long dcId, SourceMeta sourceMeta, String clusterName, long clusterId, long srcDcId) {
            DcClusterTbl dcClusterTbl = getDcClusterInfo(clusterId, srcDcId);
            if (dcClusterTbl == null) {
                getLogger().warn("[buildSourceShardMetas] dcCluster not found; clusterId={}", clusterId);
                return;
            }

            List<DcClusterShardTbl> dcClusterShardTbls = dcCluster2DcClusterShardMap.get(dcClusterTbl.getDcClusterId());
            if (dcClusterShardTbls == null) {
                return;
            }
            for (DcClusterShardTbl dcClusterShardTbl : dcClusterShardTbls) {
                ShardMeta shardMeta = getOrCreateShardMeta(sourceMeta, dcClusterShardTbl.getShardInfo());
                RedisTbl redis = dcClusterShardTbl.getRedisInfo();
                if (Server.SERVER_ROLE.KEEPER.sameRole(redis.getRedisRole())) {
                    if (dcId.equals(keeperContainerIdDcMap.get(redis.getKeepercontainerId()))) {
                        shardMeta.addKeeper(redisMetaService.getKeeperMeta(shardMeta, redis));
                    }
                }
            }

            addApplierAndAddShardIfNotExists(dcId, clusterName, clusterId, sourceMeta);
        }

        private void addApplierAndAddShardIfNotExists(Long dcId, String clusterName, long clusterId, SourceMeta sourceMeta) {
            List<Long> repIds = replDirectionTblList
                    .stream()
                    .filter(a -> clusterId == a.getClusterId() && a.getToDcId() == dcId)
                    .map(ReplDirectionTbl::getId)
                    .collect(Collectors.toList());

            List<ApplierTbl> appliers = new ArrayList<>();
            for (Long repId : repIds) {
                if (replId2AppliersMap.get(repId) != null) {
                    appliers.addAll(replId2AppliersMap.get(repId));
                }
            }

            for (ApplierTbl applierTbl : appliers) {
                ShardMeta shardMeta = getOrCreateShardMeta(sourceMeta, applierTbl.getShardInfo());
                ApplierMeta applierMeta = new ApplierMeta();
                applierMeta.setIp(applierTbl.getIp());
                applierMeta.setPort(applierTbl.getPort());
                applierMeta.setActive(applierTbl.isActive());
                applierMeta.setApplierContainerId(applierTbl.getContainerId());
                applierMeta.setTargetClusterName(getTargetClusterName(applierTbl, clusterName));
                shardMeta.addApplier(applierMeta);
            }
        }

        private String getTargetClusterName(ApplierTbl applierTbl, String clusterName) {
            if (applierTbl.getReplDirectionInfo() == null || StringUtils.isEmpty(applierTbl.getReplDirectionInfo().getTargetClusterName())) {
                return clusterName;
            }
            return applierTbl.getReplDirectionInfo().getTargetClusterName();
        }

        private void setDownstreamDcs(ClusterMeta clusterMeta, ReplDirectionTbl replDirectionTbl) {
            if (Strings.isEmpty(clusterMeta.getDownstreamDcs())) {
                clusterMeta.setDownstreamDcs(dcNameMap.get(replDirectionTbl.getToDcId()));
            } else {
                clusterMeta.setDownstreamDcs(clusterMeta.getDownstreamDcs() + "," + dcNameMap.get(replDirectionTbl.getToDcId()));
            }
        }

        private SourceMeta buildSourceMeta(ClusterMeta clusterMeta, long srcDc, long upstreamDc) {
            SourceMeta sourceMeta = new SourceMeta();

            sourceMeta.setSrcDc(dcNameMap.get(srcDc));
            sourceMeta.setUpstreamDc(dcNameMap.get(upstreamDc));
            sourceMeta.setRegion(zoneNameMap.get(dcNameZoneMap.get(dcNameMap.get(srcDc))));

            clusterMeta.addSource(sourceMeta);
            sourceMeta.setParent(clusterMeta);

            return sourceMeta;
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
