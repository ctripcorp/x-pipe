package com.ctrip.xpipe.redis.console.service.vo;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.util.Strings;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * @author chen.zhu
 * <p>
 * Apr 03, 2018
 */
public class DcMetaBuilder extends AbstractCommand<DcMeta> {

    private DcMeta dcMeta;

    private Map<Long, String> dcNameMap;

    private Map<String, Long> dcNameZoneMap;

    private Map<Long, String> zoneNameMap;

    private Map<Long, Long> keeperContainerIdDcMap;

    private Map<Long, List<ApplierTbl>> replId2AppliersMap;

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

    private ExecutorService executors;

    private RetryCommandFactory factory;

    private ConsoleConfig consoleConfig;

    private long dcId;

    private Set<String> interestClusterTypes;

    private Map<Long, List<DcClusterTbl>> cluster2DcClusterMap;

    private Map<Long, List<DcClusterShardTbl>> dcCluster2DcClusterShardMap;

    private List<DcClusterShardTbl> dcClusterShards;

    private static final String DC_NAME_DELIMITER = ",";

    public DcMetaBuilder(DcMeta dcMeta, long dcId, Set<String> clusterTypes, ExecutorService executors, RedisMetaService redisMetaService, DcClusterService dcClusterService,
                         ClusterMetaService clusterMetaService, DcClusterShardService dcClusterShardService, DcService dcService,
                         ReplDirectionService replDirectionService, ZoneService zoneService, KeeperContainerService keeperContainerService, ApplierService applierService,
                         RetryCommandFactory factory, ConsoleConfig consoleConfig) {
        this.dcMeta = dcMeta;
        this.dcId = dcId;
        this.interestClusterTypes = clusterTypes;
        this.executors = executors;
        this.redisMetaService = redisMetaService;
        this.dcClusterService = dcClusterService;
        this.clusterMetaService = clusterMetaService;
        this.dcClusterShardService = dcClusterShardService;
        this.dcService = dcService;
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
        parallelCommandChain.add(retry3TimesUntilSuccess(new GetAllDcClusterShardDetailCommand(dcId)));
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
                future().setSuccess(dcMeta);
            } else {
                future().setFailure(commandFuture.cause());
            }
        });
        sequenceCommandChain.execute(executors);
    }

    @Override
    protected void doReset() {
        // remove all clusters if failed
        dcMeta.getClusters().clear();
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    private  <T> Command<T> retry3TimesUntilSuccess(Command<T> command) {
        return factory.createRetryCommand(command);
    }

    @VisibleForTesting
    public ClusterMeta getOrCreateClusterMeta(ClusterTbl cluster, DcClusterTbl dcClusterInfo) {
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
                clusterMeta.setDcGroupType(dcClusterInfo == null? DcGroupType.DR_MASTER.toString(): dcClusterInfo.getGroupType());
                clusterMeta.setDcGroupName(getDcGroupName(dcClusterInfo));

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

        relatedDcClusters.forEach(dcClusterTbl ->
            allDcs.add(dcNameMap.get(dcClusterTbl.getDcId()))
        );

        if (!allDcs.isEmpty()) return String.join(DC_NAME_DELIMITER, allDcs);
        return null;
    }

    public ShardMeta getOrCreateShardMeta(String clusterId, ShardTbl shard, long sentinelId) {
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

    class GetAllDcClusterShardDetailCommand extends AbstractCommand<Void> {

        private long dcId;

        public GetAllDcClusterShardDetailCommand(long dcId) {
            this.dcId = dcId;
        }

        @Override
        protected void doExecute() throws Exception {
            try {
                dcClusterShards = dcClusterShardService.findAllByDcIdAndInClusterTypes(dcId, interestClusterTypes);
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            dcClusterShards = null;
        }

        @Override
        public String getName() {
            return GetAllDcClusterShardDetailCommand.class.getSimpleName();
        }
    }

    class DcCluster2dcClusterShardMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                dcCluster2DcClusterShardMap = Maps.newHashMap();
                List<DcClusterShardTbl> allDcClusterShards = dcClusterShardService.findAllByClusterTypes(interestClusterTypes);

                for (DcClusterShardTbl dcClusterShardTbl : allDcClusterShards) {
                    List<DcClusterShardTbl> dcClusterShardTbls = MapUtils.getOrCreate(dcCluster2DcClusterShardMap,
                            dcClusterShardTbl.getDcClusterId(), LinkedList::new);
                    dcClusterShardTbls.add(dcClusterShardTbl);
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
                for (ApplierTbl applierTbl : applierTblList) {
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
                for (DcClusterShardTbl dcClusterShard : dcClusterShards) {
                    ClusterMeta clusterMeta = getOrCreateClusterMeta(dcClusterShard.getClusterInfo(), getDcClusterInfo(dcClusterShard.getClusterInfo().getId(), dcId));
                    ShardMeta shardMeta = getOrCreateShardMeta(clusterMeta.getId(),
                            dcClusterShard.getShardInfo(), dcClusterShard.getSetinelId());

                    RedisTbl redis = dcClusterShard.getRedisInfo();
                    if (Server.SERVER_ROLE.KEEPER.sameRole(redis.getRedisRole())) {
                        if (dcId == keeperContainerIdDcMap.get(redis.getKeepercontainerId())) {
                            shardMeta.addKeeper(redisMetaService.getKeeperMeta(shardMeta, redis));
                        }
                    } else {
                        shardMeta.addRedis(redisMetaService.getRedisMeta(shardMeta, redis));
                    }
                }
                buildHeteroMeta();

                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        private void buildHeteroMeta() {

            List<ReplDirectionTbl> toCurrentDcOrFromCurrentDcList = replDirectionTblList
                    .stream()
                    .filter(a -> a.getFromDcId() == dcId || a.getToDcId() == dcId)
                    .collect(Collectors.toList());

            for (ReplDirectionTbl replDirection : toCurrentDcOrFromCurrentDcList) {
                long clusterId = replDirection.getClusterId();
                DcClusterTbl dcClusterTbl = getDcClusterInfo(clusterId, dcId);
                ClusterMeta clusterMeta = getOrCreateClusterMeta(replDirection.getClusterInfo(), dcClusterTbl);
                if (dcClusterTbl == null) {
                    getLogger().warn("[buildHeteroMeta] dcCluster not found; clusterId={}", clusterId);
                    continue;
                }
                if (replDirection.getToDcId() == dcId) {
                    if (DcGroupType.isSameGroupType(dcClusterTbl.getGroupName(), DcGroupType.MASTER)) {
                        SourceMeta sourceMeta = buildSourceMeta(clusterMeta, replDirection.getSrcDcId(), replDirection.getFromDcId());
                        buildSourceShardMetas(sourceMeta, clusterMeta.getId(), clusterId, replDirection.getSrcDcId());
                    }
                }
                if (replDirection.getFromDcId() == dcId) {
                    setDownstreamDcs(clusterMeta, replDirection);
                }
            }
        }

        private void buildSourceShardMetas(SourceMeta sourceMeta, String clusterName, long clusterId, long srcDcId) {
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
                    if (dcId == keeperContainerIdDcMap.get(redis.getKeepercontainerId())) {
                        shardMeta.addKeeper(redisMetaService.getKeeperMeta(shardMeta, redis));
                    }
                }
            }

            addApplierAndAddShardIfNotExists(clusterName, clusterId, sourceMeta);
        }

        private void addApplierAndAddShardIfNotExists(String clusterName, long clusterId, SourceMeta sourceMeta) {
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
            if (applierTbl.getReplDirectionInfo() == null || applierTbl.getReplDirectionInfo().getTargetClusterName() == null) {
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

        private DcClusterTbl getDcClusterInfo(long clusterId, long dcId) {
            for(DcClusterTbl dcClusterTbl: cluster2DcClusterMap.get(clusterId)) {
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

    private String getDcGroupName(DcClusterTbl dcClusterInfo) {
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

    public DcMetaBuilder setDcClusterShards(List<DcClusterShardTbl> dcClusterShards) {
        this.dcClusterShards = dcClusterShards;
        return this;
    }
}
