package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.cluster.Hints;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.AbstractMetaService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.ShardMetaService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.SourceMeta;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author shyin
 * <p>
 * Aug 17, 2016
 */
@Service
public class ClusterMetaServiceImpl extends AbstractMetaService implements ClusterMetaService {

    @Autowired
    private AzGroupCache azGroupCache;
    @Autowired
    private DcService dcService;
    @Autowired
    private ClusterService clusterService;
    @Autowired
    private ShardService shardService;
    @Autowired
    private DcClusterService dcClusterService;
    @Autowired
    private ShardMetaService shardMetaService;
    @Autowired
    private MigrationService migrationService;
    @Autowired
    private ReplDirectionService replDirectionService;
    @Autowired
    private ZoneService zoneService;
    @Autowired
    private KeeperContainerService keeperContainerService;
    @Autowired
    private ApplierService applierService;
    @Autowired
    private AzGroupClusterRepository azGroupClusterRepository;

    private static final String DC_NAME_DELIMITER = ",";

    @Override
    public ClusterMeta loadClusterMeta(DcMeta dcMeta, ClusterTbl clusterTbl, DcMetaQueryVO dcMetaQueryVO) {
        ClusterMeta clusterMeta = new ClusterMeta();
        clusterTbl.setActivedcId(getClusterMetaCurrentPrimaryDc(dcMetaQueryVO.getCurrentDc(), clusterTbl));

        clusterMeta.setId(clusterTbl.getClusterName());
        clusterMeta.setDbId(clusterTbl.getId());
        loadDcs(dcMetaQueryVO, clusterTbl, clusterMeta);
        clusterMeta.setParent(dcMeta);

        for (ShardTbl shard : dcMetaQueryVO.getShardMap().get(clusterTbl.getClusterName())) {
            clusterMeta.addShard(shardMetaService.loadShardMeta(clusterMeta, clusterTbl, shard, dcMetaQueryVO));
        }

        return clusterMeta;
    }

    private void loadDcs(DcMetaQueryVO dcMetaQueryVO, ClusterTbl clusterTbl, ClusterMeta clusterMeta) {
        String activeDc = null;
        List<String> backupDcs = new ArrayList<>();
        List<String> allDcs = new ArrayList<>();
        for (DcClusterTbl dcCluster : dcMetaQueryVO.getAllDcClusterMap().get(clusterTbl.getId())) {
            String dcName = dcMetaQueryVO.getAllDcs().get(dcCluster.getDcId()).getDcName();
            allDcs.add(dcName);
            if (dcCluster.getDcId() == clusterTbl.getActivedcId()) {
                activeDc = dcName;
            } else {
                backupDcs.add(dcName);
            }
        }

        if (!ClusterType.lookup(clusterTbl.getClusterType()).supportMultiActiveDC()) {
            clusterMeta.setActiveDc(activeDc);
            if (!backupDcs.isEmpty()) clusterMeta.setBackupDcs(String.join(DC_NAME_DELIMITER, backupDcs));
        } else {
            clusterMeta.setDcs(String.join(DC_NAME_DELIMITER, allDcs));
        }
    }

    @Override
    public ClusterMeta getClusterMeta(final String dcName, final String clusterName) {
        DcTbl dc = dcService.find(dcName);
        ClusterTbl cluster = clusterService.find(clusterName);
        DcClusterTbl dcCluster = dcClusterService.find(dcName, clusterName);
        if (dc == null || cluster == null || dcCluster == null) {
            throw new DataNotFoundException(String.format("not find dc cluster %s %s", dcName, clusterName));
        }

        AzGroupClusterEntity azGroupCluster = azGroupClusterRepository.selectById(dcCluster.getAzGroupClusterId());
        List<DcTbl> clusterRelatedDcs = dcService.findClusterRelatedDc(clusterName);
        ClusterMeta clusterMeta =
            generateBasicClusterMeta(dc, cluster, dcCluster, azGroupCluster, clusterRelatedDcs);

        List<ShardTbl> shards = shardService.findAllByClusterName(clusterName);
        Map<Long, Long> keeperContainerId2DcMap = keeperContainerService.keeperContainerIdDcMap();
        if (null != shards) {
            for (ShardTbl shard : shards) {
                ShardMeta shardMeta = shardMetaService.getShardMeta(dc, cluster, shard, keeperContainerId2DcMap);
                if (shardMeta != null) {
                    clusterMeta.addShard(shardMeta);
                }
            }
        }
        ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
        if (ClusterType.ONE_WAY == clusterType) {
            Map<Long, DcClusterTbl> dcClusterMap = generateDcClusterMap(cluster.getId());
            generateAsymmetricMeta(clusterMeta, dc.getId(), azGroupCluster, clusterRelatedDcs, dcClusterMap, shards,
                cluster, keeperContainerId2DcMap);
            List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterId(cluster.getId());
            addClusterHints(clusterMeta, azGroupClusters, shards);
        }

        // 异构集群更新类型为az group对应类型
        if (ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.HETERO)
            && !StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
            ClusterType azGroupType = ClusterType.lookup(clusterMeta.getAzGroupType());
            clusterMeta.setType(azGroupType.toString());
            clusterMeta.setAzGroupType(null);
        }
        return clusterMeta;
    }

    private Map<Long, DcClusterTbl> generateDcClusterMap(long clusterDbId) {
        List<DcClusterTbl> relatedDcClusterInfo =
            dcClusterService.findByClusterIds(Collections.singletonList(clusterDbId));
        return relatedDcClusterInfo.stream().collect(Collectors.toMap(DcClusterTbl::getDcId, Function.identity()));
    }

    @VisibleForTesting
    protected ClusterMeta generateBasicClusterMeta(DcTbl dc, ClusterTbl cluster, DcClusterTbl dcCluster,
        AzGroupClusterEntity azGroupCluster, List<DcTbl> clusterRelatedDcs) {
        ClusterMeta clusterMeta = new ClusterMeta();
        clusterMeta.setId(cluster.getClusterName());
        clusterMeta.setDbId(cluster.getId());
        clusterMeta.setType(cluster.getClusterType());
        clusterMeta.setActiveRedisCheckRules(dcCluster.getActiveRedisCheckRules());
        clusterMeta.setClusterDesignatedRouteIds(cluster.getClusterDesignatedRouteIds());
        clusterMeta.setOrgId(Math.toIntExact(cluster.getClusterOrgId()));
        cluster.setActivedcId(getClusterMetaCurrentPrimaryDc(dc, cluster));
        clusterMeta.setBackupDcs("");
        clusterMeta.setDownstreamDcs("");

        // TODO: 下一版本删除DcGroup逻辑
        clusterMeta.setDcGroupName(getDcGroupName(dc.getDcName(), dcCluster));
        if (ClusterType.ONE_WAY.name().equalsIgnoreCase(cluster.getClusterType())) {
            if (dcCluster.getGroupType() == null) {
                clusterMeta.setDcGroupType(DcGroupType.DR_MASTER.toString());
            } else {
                clusterMeta.setDcGroupType(dcCluster.getGroupType());
            }
        }

        ClusterType clusterType = ClusterType.lookup(cluster.getClusterType());
        if (clusterType.supportMultiActiveDC()) {
            clusterMeta.setDcs(StringUtil.join(",", DcTbl::getDcName, clusterRelatedDcs));
        } else {
            DcTbl activeDc = clusterRelatedDcs.stream()
                .filter(dcTbl -> dcTbl.getId() == cluster.getActivedcId())
                .findFirst().orElse(null);
            if (activeDc == null) {
                throw new IllegalStateException("active dc not found");
            }
            clusterMeta.setActiveDc(activeDc.getDcName());
            List<DcTbl> backupDcs = clusterRelatedDcs.stream()
                .filter(dcTbl -> dcTbl.getId() != cluster.getActivedcId())
                .collect(Collectors.toList());
            clusterMeta.setBackupDcs(StringUtil.join(",", DcTbl::getDcName, backupDcs));
        }

        if (azGroupCluster != null) {
            AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
            clusterMeta.setAzGroupName(azGroup.getName());
            clusterMeta.setAzGroupType(azGroupCluster.getAzGroupClusterType());

            if (clusterType == ClusterType.HETERO) {
                DcTbl activeAzTbl = dcService.find(azGroupCluster.getActiveAzId());
                if (activeAzTbl != null) {
                    String activeAz = activeAzTbl.getDcName();
                    clusterMeta.setActiveDc(activeAz);
                    List<String> azs = azGroup.getAzsAsList();
                    azs.remove(activeAz);
                    clusterMeta.setBackupDcs(String.join(DC_NAME_DELIMITER, azs));
                }
            }
        }

        return clusterMeta;
    }

    /**
     * Perform differently with migrating cluster
     **/
    @Override
    public long getClusterMetaCurrentPrimaryDc(DcTbl dcInfo, ClusterTbl clusterInfo) {
        if (ClusterStatus.isSameClusterStatus(clusterInfo.getStatus(), ClusterStatus.Migrating)) {
            MigrationClusterTbl migrationCluster = migrationService.findMigrationCluster(clusterInfo.getMigrationEventId(), clusterInfo.getId());
            if (migrationCluster != null && dcInfo.getId() == migrationCluster.getDestinationDcId()) {
                logger.info("[getClusterMetaCurrentPrimaryDc][{}][{}] migrating, return dst dc {}",
                        dcInfo.getDcName(), clusterInfo.getClusterName(), migrationCluster.getDestinationDcId());
                return migrationCluster.getDestinationDcId();
            } else if (null == migrationCluster) {
                logger.info("[getClusterMetaCurrentPrimaryDc][{}][{}] migrating but no event {}, return origin active dc {}",
                        dcInfo.getDcName(), clusterInfo.getClusterName(), clusterInfo.getMigrationEventId(), clusterInfo.getActivedcId());
            }
        }
        return clusterInfo.getActivedcId();
    }

    public void setMigrationService(MigrationService migrationService) {
        this.migrationService = migrationService;
    }

    private String getDcGroupName(String dcName, DcClusterTbl dcClusterInfo) {
        if (dcClusterInfo == null || dcClusterInfo.getGroupName() == null) {
            return dcName;
        }

        return dcClusterInfo.getGroupName();
    }

    @VisibleForTesting
    protected void generateAsymmetricMeta(ClusterMeta clusterMeta, long dcId, AzGroupClusterEntity azGroupCluster,
        List<DcTbl> dcList, Map<Long, DcClusterTbl> dcClusterMap, List<ShardTbl> shards, ClusterTbl clusterInfo,
        Map<Long, Long> keeperContainerId2DcMap) {

        List<ReplDirectionTbl> replDirections =
            replDirectionService.findAllReplDirectionTblsByClusterWithSrcDcAndFromDc(clusterMeta.getDbId());
        if (replDirections == null) {
            return;
        }
        for (ReplDirectionTbl replDirectionTbl : replDirections) {
            if (replDirectionTbl.getToDcId() == dcId) {
                ClusterType azGroupClusterType = ClusterType.lookup(azGroupCluster.getAzGroupClusterType());
                if (azGroupClusterType == ClusterType.SINGLE_DC) {
                    SourceMeta sourceMeta = buildSourceMeta(clusterMeta, replDirectionTbl.getSrcDcId(),
                            replDirectionTbl.getFromDcId(), dcList);
                    buildSourceShardMetas(sourceMeta, clusterMeta.getDbId(), replDirectionTbl.getSrcDcId(), dcId,
                            dcClusterMap, dcList, shards, clusterInfo, keeperContainerId2DcMap, replDirectionTbl.getId());
                }
            }
            if (replDirectionTbl.getFromDcId() == dcId) {
                setDownstreamDcs(clusterMeta, replDirectionTbl.getToDcId(), dcList);
            }
        }
    }

    private void setDownstreamDcs(ClusterMeta clusterMeta, long dcId, List<DcTbl> dcList) {

        String downstreamDc = getDcName(dcId, dcList);

        if (Strings.isNullOrEmpty(clusterMeta.getDownstreamDcs())) {
            clusterMeta.setDownstreamDcs(downstreamDc);
        } else {
            clusterMeta.setDownstreamDcs(clusterMeta.getDownstreamDcs() + "," + downstreamDc);
        }
    }

    private DcTbl getDcById(long dcId, List<DcTbl> dcList) {
        for (DcTbl dcTbl : dcList) {
            if (dcTbl.getId() == dcId) {
                return dcTbl;
            }
        }
        return null;
    }

    private long getZoneId(long dcId, List<DcTbl> dcList) {
        DcTbl dcTbl = getDcById(dcId, dcList);
        return dcTbl == null? 0L: dcTbl.getZoneId();
    }

    private String getDcName(long dcId, List<DcTbl> dcList) {
        DcTbl dcTbl = getDcById(dcId, dcList);
        return dcTbl == null? "": dcTbl.getDcName();
    }

    @VisibleForTesting
    protected SourceMeta buildSourceMeta(ClusterMeta clusterMeta, long srcDcId, long fromDcId, List<DcTbl> dcList) {
        SourceMeta sourceMeta = new SourceMeta();

        sourceMeta.setSrcDc(getDcName(srcDcId, dcList));
        sourceMeta.setUpstreamDc(getDcName(fromDcId, dcList));
        sourceMeta.setRegion(getRegion(srcDcId, dcList));

        clusterMeta.addSource(sourceMeta);
        sourceMeta.setParent(clusterMeta);

        return sourceMeta;
    }

    private String getRegion(long dcId, List<DcTbl> dcList) {
        long zoneId = getZoneId(dcId, dcList);

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);
        Future<ZoneTbl> futureZone = fixedThreadPool.submit(() -> zoneService.findById(zoneId));

        ZoneTbl zone;
        try {
            zone = futureZone.get();
        } catch (ExecutionException e) {
            throw new DataNotFoundException("Cannot construct region", e);
        } catch (InterruptedException e) {
            throw new ServerException("Concurrent region execution failed.", e);
        } finally {
            fixedThreadPool.shutdown();
        }

        return zone == null? "": zone.getZoneName();
    }

    private void buildSourceShardMetas(SourceMeta sourceMeta, long clusterId, long srcDcId, long currentDcId,
                                       Map<Long, DcClusterTbl> dcClusterMap, List<DcTbl> dcList, List<ShardTbl> shards,
                                       ClusterTbl clusterInfo, Map<Long, Long> keeperContainerId2DcMap, long replId) {
        DcClusterTbl dcClusterTbl = dcClusterMap.get(srcDcId);
        if (dcClusterTbl == null) {
            logger.warn("[buildSourceShardMetas] dcCluster not found; clusterId={}", clusterId);
            return;
        }

        if (null != shards) {
            for (ShardTbl shard : shards) {
                ShardMeta shardMeta = shardMetaService.getSourceShardMeta(getDcById(srcDcId, dcList), getDcById(currentDcId, dcList),
                        clusterInfo, shard, dcClusterTbl, keeperContainerId2DcMap, replId);
                if (shardMeta != null) {
                    sourceMeta.addShard(shardMeta);
                }
            }
        }
    }

    private void addClusterHints(ClusterMeta clusterMeta, List<AzGroupClusterEntity> azGroupClusters, List<ShardTbl> shards) {
        if (clusterHasApplier(shards)) {
            clusterMeta.setHints(Hints.append(clusterMeta.getHints(), Hints.APPLIER_IN_CLUSTER));
        }
        if (clusterHasMasterDc(azGroupClusters)) {
            clusterMeta.setHints(Hints.append(clusterMeta.getHints(), Hints.MASTER_DC_IN_CLUSTER));
        }
    }

    private boolean clusterHasMasterDc(List<AzGroupClusterEntity> azGroupClusters) {
        for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
            if (ClusterType.isSameClusterType(azGroupCluster.getAzGroupClusterType(), ClusterType.SINGLE_DC)) {
                return true;
            }
        }
        return false;
    }

    private boolean clusterHasApplier(List<ShardTbl> shards) {
        List<Long> shardIds = shards.stream().map(ShardTbl::getId).collect(Collectors.toList());
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);

        Future<List<ApplierTbl>> futureAppliers = fixedThreadPool.submit( ()
                -> applierService.findAppliersByShardIds(shardIds));

        List<ApplierTbl> appliers;
        try {
            appliers = futureAppliers.get();
        } catch (ExecutionException e) {
            throw new DataNotFoundException("Cannot construct applier-meta by shardIds", e);
        } catch (InterruptedException e) {
            throw new ServerException("Concurrent clusterHasApplier execution failed.", e);
        } finally {
            fixedThreadPool.shutdown();
        }

        return appliers != null && !appliers.isEmpty();
    }

    @VisibleForTesting
    protected void setZoneService(ZoneService zoneService) {
        this.zoneService = zoneService;
    }

    @VisibleForTesting
    protected void setReplDirectionService(ReplDirectionService replDirectionService) {
        this.replDirectionService = replDirectionService;
    }
}
