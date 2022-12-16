package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.cluster.Hints;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
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
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(6);

        Future<DcTbl> future_dcInfo = fixedThreadPool.submit(new Callable<DcTbl>() {
            @Override
            public DcTbl call() throws Exception {
                return dcService.find(dcName);
            }
        });
        Future<ClusterTbl> future_clusterInfo = fixedThreadPool.submit(new Callable<ClusterTbl>() {
            @Override
            public ClusterTbl call() throws Exception {
                return clusterService.find(clusterName);
            }
        });
        Future<DcClusterTbl> future_dcClusterInfo = fixedThreadPool.submit(new Callable<DcClusterTbl>() {
            @Override
            public DcClusterTbl call() throws Exception {
                return dcClusterService.find(dcName, clusterName);
            }
        });
        Future<List<ShardTbl>> future_shardsInfo = fixedThreadPool.submit(new Callable<List<ShardTbl>>() {
            @Override
            public List<ShardTbl> call() throws Exception {
                return shardService.findAllByClusterName(clusterName);
            }
        });
        Future<List<DcTbl>> future_clusterRelatedDc = fixedThreadPool.submit(new Callable<List<DcTbl>>() {
            @Override
            public List<DcTbl> call() throws Exception {
                return dcService.findClusterRelatedDc(clusterName);
            }
        });
        Future<Map<Long, Long>> future_keeperContainerId2DcMap = fixedThreadPool.submit(() ->
                keeperContainerService.keeperContainerIdDcMap());

        ClusterMeta clusterMeta = new ClusterMeta();
        clusterMeta.setId(clusterName);
        try {
            DcTbl dcInfo = future_dcInfo.get();
            ClusterTbl clusterInfo = future_clusterInfo.get();
            DcClusterTbl dcClusterInfo = future_dcClusterInfo.get();
            List<DcTbl> dcList = future_clusterRelatedDc.get();
            Map<Long, DcClusterTbl> dcClusterMap = generateDcClusterMap(clusterInfo.getId());
            Map<Long, Long> keeperContainerId2DcMap = future_keeperContainerId2DcMap.get();

            generateBasicClusterMeta(clusterMeta, dcName, clusterName, dcInfo, clusterInfo,
                    dcClusterInfo, dcList, dcClusterMap);

            List<ShardTbl> shards = future_shardsInfo.get();
            if (null != shards) {
                for (ShardTbl shard : shards) {
                    ShardMeta shardMeta = shardMetaService.getShardMeta(dcInfo, clusterInfo, shard, keeperContainerId2DcMap);
                    if (shardMeta != null) {
                        clusterMeta.addShard(shardMeta);
                    }
                }
            }
            if (ClusterType.ONE_WAY.name().equalsIgnoreCase(clusterMeta.getType())) {
                generateHeteroMeta(clusterMeta, dcInfo.getId(), dcClusterInfo, dcList, dcClusterMap, shards, clusterInfo, keeperContainerId2DcMap);
                addClusterHints(clusterMeta, dcClusterMap, shards);
            }
        } catch (ExecutionException e) {
            throw new DataNotFoundException("Cannot construct cluster-meta", e);
        } catch (InterruptedException e) {
            throw new ServerException("Concurrent execution failed.", e);
        } finally {
            fixedThreadPool.shutdown();
        }

        return clusterMeta;
    }

    private Map<Long, DcClusterTbl> generateDcClusterMap(long clusterDbId) {

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);

        Future<List<DcClusterTbl>> futureRelatedDcClusterInfo = fixedThreadPool.submit(
                () -> dcClusterService.findByClusterIds(Collections.singletonList(clusterDbId)));

        Map<Long, DcClusterTbl> relatedDcClusterMap;

        try {
            List<DcClusterTbl> relatedDcClusterInfo = futureRelatedDcClusterInfo.get();
            relatedDcClusterMap = relatedDcClusterInfo.stream().collect(Collectors.toMap(DcClusterTbl::getDcId, Function.identity()));
        } catch (ExecutionException e) {
            throw new DataNotFoundException("Cannot gen hetero-meta", e);
        } catch (InterruptedException e) {
            throw new ServerException("Concurrent hetero-meta execution failed.", e);
        } finally {
            fixedThreadPool.shutdown();
        }

        return relatedDcClusterMap == null? new HashMap<>(): relatedDcClusterMap;
    }

    @VisibleForTesting
    protected void generateBasicClusterMeta(ClusterMeta clusterMeta, String dcName, String clusterName,
                                          DcTbl dcInfo, ClusterTbl clusterInfo, DcClusterTbl dcClusterInfo,
                                          List<DcTbl> clusterRelatedDc, Map<Long, DcClusterTbl> dcClusterMap) {

        if (null == dcInfo || null == clusterInfo || null == dcClusterInfo)
            throw new DataNotFoundException(String.format("unfound dc cluster %s %s", dcName, clusterName));

        clusterMeta.setId(clusterInfo.getClusterName());
        clusterMeta.setDbId(clusterInfo.getId());
        clusterMeta.setType(clusterInfo.getClusterType());
        clusterMeta.setActiveRedisCheckRules(dcClusterInfo.getActiveRedisCheckRules());
        clusterMeta.setClusterDesignatedRouteIds(clusterInfo.getClusterDesignatedRouteIds());
        clusterMeta.setOrgId(Math.toIntExact(clusterInfo.getClusterOrgId()));
        clusterInfo.setActivedcId(getClusterMetaCurrentPrimaryDc(dcInfo, clusterInfo));
        clusterMeta.setBackupDcs("");
        clusterMeta.setDownstreamDcs("");
        clusterMeta.setDcGroupName(getDcGroupName(dcName, dcClusterInfo));

        if (ClusterType.ONE_WAY.name().equalsIgnoreCase(clusterInfo.getClusterType())) {
            if (dcClusterInfo.getGroupType() == null) {
                clusterMeta.setDcGroupType(DcGroupType.DR_MASTER.toString());
            } else {
                clusterMeta.setDcGroupType(dcClusterInfo.getGroupType());
            }
        }

        if (ClusterType.lookup(clusterInfo.getClusterType()).supportMultiActiveDC()) {
            clusterMeta.setDcs(StringUtil.join(",", dcTbl -> dcTbl.getDcName(), clusterRelatedDc));
        } else {
            for (DcTbl dc : clusterRelatedDc) {
                if (dc.getId() == clusterInfo.getActivedcId()) {
                    clusterMeta.setActiveDc(dc.getDcName());
                } else {
                    DcClusterTbl dcClusterTbl = dcClusterMap.get(dc.getId());
                    if (dcClusterTbl != null && !DcGroupType.isNullOrDrMaster(dcClusterTbl.getGroupType())) {
                        continue;
                    }
                    if (Strings.isNullOrEmpty(clusterMeta.getBackupDcs())) {
                        clusterMeta.setBackupDcs(dc.getDcName());
                    } else {
                        clusterMeta.setBackupDcs(clusterMeta.getBackupDcs() + "," + dc.getDcName());
                    }
                }
            }
        }
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
    protected void generateHeteroMeta(ClusterMeta clusterMeta, long dcId, DcClusterTbl dcClusterInfo, List<DcTbl> dcList,
                                    Map<Long, DcClusterTbl> dcClusterMap, List<ShardTbl> shards, ClusterTbl clusterInfo,
                                    Map<Long, Long> keeperContainerId2DcMap) {
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);

        Future<List<ReplDirectionTbl>> futureReplDirectionList = fixedThreadPool.submit(
                () -> replDirectionService.findAllReplDirectionTblsByClusterWithSrcDcAndFromDc(clusterMeta.getDbId()));

        try {
            for (ReplDirectionTbl replDirectionTbl : futureReplDirectionList.get()) {
                if (replDirectionTbl.getToDcId() == dcId) {
                    if (DcGroupType.isSameGroupType(dcClusterInfo.getGroupType(), DcGroupType.MASTER)) {
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
        } catch (ExecutionException e) {
            throw new DataNotFoundException("Cannot gen hetero-meta", e);
        } catch (InterruptedException e) {
            throw new ServerException("Concurrent hetero-meta execution failed.", e);
        } finally {
            fixedThreadPool.shutdown();
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

    private void addClusterHints(ClusterMeta clusterMeta, Map<Long, DcClusterTbl> dcClusterMap, List<ShardTbl> shards) {
        if (clusterHasApplier(shards)) {
            clusterMeta.setHints(Hints.append(clusterMeta.getHints(), Hints.APPLIER_IN_CLUSTER));
        }
        if (clusterHasMasterDc(dcClusterMap)) {
            clusterMeta.setHints(Hints.append(clusterMeta.getHints(), Hints.MASTER_DC_IN_CLUSTER));
        }
    }

    private boolean clusterHasMasterDc(Map<Long, DcClusterTbl> dcClusterMap) {
        for (DcClusterTbl dcClusterTbl : dcClusterMap.values()) {
            if (DcGroupType.MASTER.name().equalsIgnoreCase(dcClusterTbl.getGroupType())) {
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
