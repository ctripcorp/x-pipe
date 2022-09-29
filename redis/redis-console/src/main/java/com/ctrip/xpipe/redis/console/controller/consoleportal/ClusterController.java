package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitorManager;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthState;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ClusterListUnhealthyClusterModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.util.SetOperationUtil;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.unidal.dal.jdbc.DalException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zhangle
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class ClusterController extends AbstractConsoleController {

    @Autowired
    private ConsoleConfig config;
    @Autowired
    private DcService dcService;
    @Autowired
    private ClusterService clusterService;
    @Autowired
    private DcClusterService dcClusterService;
    @Autowired
    private ClusterHealthMonitorManager clusterHealthMonitorManager;
    @Autowired
    private ReplDirectionService replDirectionService;
    @Autowired
    private ShardService shardService;
    @Autowired
    private SentinelBalanceService sentinelBalanceService;
    @Autowired
    private ApplierService applierService;

    private SetOperationUtil setOperator = new SetOperationUtil();

    private Comparator<ReplDirectionTbl> replDirectionTblComparator = new Comparator<ReplDirectionTbl>() {
        @Override
        public int compare(ReplDirectionTbl o1, ReplDirectionTbl o2) {
            if (o1 != null && o2 != null
                    && ObjectUtils.equals(o1.getId(), o2.getId())
                    && ObjectUtils.equals(o1.getTargetClusterName(), o2.getTargetClusterName())) {
                return 0;
            }
            return -1;
        }
    };

    private Comparator<DcClusterModel> dcClusterModelComparator = new Comparator<DcClusterModel>() {
        @Override
        public int compare(DcClusterModel o1, DcClusterModel o2) {
            if (o1 != null && o2 != null && o1.getDcCluster() != null && o2.getDcCluster() != null
                    && ObjectUtils.equals(o1.getDcCluster().getDcId(), o2.getDcCluster().getDcId())
                    && ObjectUtils.equals(o1.getDcCluster().getClusterId(), o2.getDcCluster().getClusterId())
                    && ObjectUtils.equals(o1.getDcCluster().getGroupName(), o2.getDcCluster().getGroupName())
                    && ObjectUtils.equals(o1.getDcCluster().isGroupType(), o2.getDcCluster().isGroupType())) {
                return 0;
            }
            return -1;
        }
    };

    private Comparator<ShardTbl> shardTblComparator = new Comparator<ShardTbl>() {
        @Override
        public int compare(ShardTbl o1, ShardTbl o2) {
            if (o1 != null && o2 != null
                    && ObjectUtils.equals(o1.getShardName(), o2.getShardName())
                    && ObjectUtils.equals(o1.getSetinelMonitorName(), o2.getSetinelMonitorName())) {
                return 0;
            }
            return -1;
        }
    };

    private static final String DEFAULT_HICKWALL_CLUSTER_METRIC_FORMAT
            = "http://127.0.0.1/grafanav2/d/8uhYAmc7k/redisshuang-xiang-tong-bu-ji-qun-de-mo-ban?var-cluster=%s";

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs", method = RequestMethod.GET)
    public List<DcTbl> findClusterDcs(@PathVariable String clusterName) {
        return valueOrEmptySet(DcTbl.class, dcService.findClusterRelatedDc(clusterName));
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public ClusterTbl loadCluster(@PathVariable String clusterName) {
        return valueOrDefault(ClusterTbl.class, clusterService.findClusterAndOrg(clusterName));
    }

    @RequestMapping(value = "/clusters/by/names", method = RequestMethod.POST)
    public List<ClusterTbl> loadClusters(@RequestBody List<String> clusterNames) {
        List<ClusterTbl> clusters = clusterNames.stream()
                .map(name -> clusterService.findClusterAndOrg(name))
                .collect(Collectors.toList());
        List<Long> clusterIds = clusters.stream().map(ClusterTbl::getId).collect(Collectors.toList());
        List<DcClusterTbl> dcClusters = dcClusterService.findByClusterIds(clusterIds);
        return valueOrEmptySet(ClusterTbl.class, joinClusterAndDcCluster(clusters, dcClusters));
    }

    @RequestMapping(value = "/clusters/all", method = RequestMethod.GET)
    public List<ClusterTbl> findAllClusters(@RequestParam(required = false) String activeDcName) {
        if (StringUtil.isEmpty(activeDcName)) {
            return valueOrEmptySet(ClusterTbl.class, clusterService.findAllClustersWithOrgInfo());
        } else {
            DcTbl dc = dcService.findByDcName(activeDcName);
            if (dc != null) {
                List<ClusterTbl> clusters = clusterService.findClustersWithOrgInfoByActiveDcId(dc.getId());

                if (!clusters.isEmpty()) {
                    List<Long> clusterIds = new ArrayList<Long>(clusters.size());
                    for (ClusterTbl c : clusters) {
                        clusterIds.add(c.getId());
                    }

                    List<DcClusterTbl> dcClusters = dcClusterService.findByClusterIds(clusterIds);

                    return joinClusterAndDcCluster(clusters, dcClusters);
                }
            }
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/clusters/unhealthy", method = RequestMethod.GET)
    public List<ClusterListUnhealthyClusterModel> findUnhealthyClusters() {
        return valueOrEmptySet(ClusterListUnhealthyClusterModel.class, clusterService.findUnhealthyClusters());
    }

    @RequestMapping(value = "/clusters/error/migrating", method = RequestMethod.GET)
    public List<ClusterTbl> findErrorMigratingClusters() {
        return valueOrEmptySet(ClusterTbl.class, clusterService.findErrorMigratingClusters());
    }

    @RequestMapping(value = "/clusters/migrating", method = RequestMethod.GET)
    public List<ClusterTbl> findMigratingClusters() {
        return valueOrEmptySet(ClusterTbl.class, clusterService.findMigratingClusters());
    }

    @RequestMapping(value = "/clusters/reset/status", method = RequestMethod.POST)
    public RetMessage resetClustersStatus(@RequestBody List<Long> ids) {
        if (ids.size() == 0) {
            return RetMessage.createFailMessage("zero clusters to reset.");
        }
        try {
            clusterService.resetClustersStatus(ids);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    private List<ClusterTbl> joinClusterAndDcCluster(List<ClusterTbl> clusters, List<DcClusterTbl> dcClusters) {
        Map<Long, ClusterTbl> id2Cluster = new HashMap<>();
        for (ClusterTbl c : clusters) {
            id2Cluster.put(c.getId(), c);
        }

        for (DcClusterTbl dcc : dcClusters) {
            ClusterTbl c = id2Cluster.get(dcc.getClusterId());
            if (c != null) {
                c.getDcClusterInfo().add(dcc);
            }
        }

        return clusters;
    }

    @RequestMapping(value = "/count/clusters", method = RequestMethod.GET)
    public Long getClustersCount() {
        return valueOrDefault(Long.class, clusterService.getAllCount());
    }

    @RequestMapping(value = "/clusters", method = RequestMethod.POST)
    public ClusterTbl createCluster(@RequestBody ClusterModel cluster) {
        logger.info("[Create Cluster]{}", cluster);
        if (cluster.getDcs() != null && (cluster.getDcClusters() == null || cluster.getDcClusters().isEmpty())) {
            List<DcClusterModel> dcClusters = new ArrayList<>();
            cluster.getDcs().forEach(dcTbl -> {
                DcModel dcModel = new DcModel();
                dcModel.setDc_name(dcTbl.getDcName());
                dcClusters.add(new DcClusterModel().setDc(dcModel).
                        setDcCluster(new DcClusterTbl().setGroupType(true).setGroupName(dcTbl.getDcName())));
            });
            cluster.setDcClusters(dcClusters);
        }
        return clusterService.createCluster(cluster);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.PUT)
    public void updateCluster(@PathVariable String clusterName, @RequestBody ClusterModel cluster) {
        logger.info("[Update Cluster]{},{}", clusterName, cluster);
        clusterService.updateCluster(clusterName, cluster);

        if (cluster.getDcClusters() != null) {
            updateDcClustersByDcClusterModels(cluster.getDcClusters(), cluster.getClusterTbl());
        }

        if (cluster.getReplDirections() != null) {
            updateClusterReplDirections(cluster.getClusterTbl(), cluster.getReplDirections());
        }
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.DELETE)
    public void deleteCluster(@PathVariable String clusterName) {
        logger.info("[Delete Cluster]{}", clusterName);
        clusterService.deleteCluster(clusterName);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}", method = RequestMethod.POST)
    public void bindDc(@PathVariable String clusterName, @PathVariable String dcName) {
        logger.info("[bindDc]{},{}", clusterName, dcName);
        clusterService.bindDc(new DcClusterTbl().setClusterName(clusterName).setDcName(dcName).setGroupType(true));
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}", method = RequestMethod.DELETE)
    public void unbindDc(@PathVariable String clusterName, @PathVariable String dcName) {
        logger.info("[unbindDc]{}, {}", clusterName, dcName);
        clusterService.unbindDc(clusterName, dcName);
    }

    @RequestMapping(value = "/clusters/allBind/{dcName}", method = RequestMethod.GET)
    public List<ClusterTbl> findClustersByDcNameBind(@PathVariable String dcName) {
        logger.info("[findClustersByDcId]dcName: {}", dcName);
        return clusterService.findAllClusterByDcNameBind(dcName);
    }

    @RequestMapping(value = "/clusters/allBind/{dcName}/{clusterType}", method = RequestMethod.GET)
    public List<ClusterTbl> findClustersByDcNameBindAndType(@PathVariable String dcName, @PathVariable String clusterType) {
        logger.info("[findClustersByDcNameBindAndType]dcName: {}, clusterType: {}", dcName, clusterType);
        return clusterService.findAllClusterByDcNameBindAndType(dcName, clusterType);
    }

    @RequestMapping(value = "/clusters/activeDc/{dcName}", method = RequestMethod.GET)
    public List<ClusterTbl> findClustersByActiveDcName(@PathVariable String dcName) {
        logger.info("[findClustersByActiveDcName]dcName: {}", dcName);
        return clusterService.findActiveClustersByDcName(dcName);
    }

    @RequestMapping(value = "/clusters/activeDc/{dcName}/{clusterType}", method = RequestMethod.GET)
    public List<ClusterTbl> findClustersByActiveDcNameAndType(@PathVariable String dcName, @PathVariable String clusterType) {
        logger.info("[findClustersByActiveDcNameAndType]dcName: {}, clusterType: {}", dcName, clusterType);
        return clusterService.findActiveClustersByDcNameAndType(dcName, clusterType);
    }

    @RequestMapping(value = "/clusters/master/unhealthy/{level}", method = RequestMethod.GET)
    public Set<String> getMasterUnhealthyClusters(@PathVariable String level) {
        logger.info("[getMasterUnhealthyClusters]level: {}", level);
        ClusterHealthState state = null;
        try {
            state = ClusterHealthState.valueOf(level.toUpperCase());
        } catch (Exception e) {
            logger.error("[getMasterUnhealthyClusters] level not matched: {}", level);
        }
        if (state != null) {
            return clusterHealthMonitorManager.getWarningClusters(state);
        }
        return Sets.newHashSet();
    }

    @RequestMapping(value = "/clusters/keepercontainer/{containerId}", method = RequestMethod.GET)
    public List<ClusterTbl> findClusterByKeeperContainer(@PathVariable Long containerId) {
        if (null == containerId || containerId <= 0) return Collections.emptyList();
        return clusterService.findAllClusterByKeeperContainer(containerId);
    }


    @RequestMapping(value = "/cluster/hickwall/" + CLUSTER_NAME_PATH_VARIABLE + "/{clusterType}", method = RequestMethod.GET)
    public RetMessage getClusterHickwallUrl(@PathVariable String clusterName, @PathVariable String clusterType) {
        String hickwallAddress = config.getHickwallClusterMetricFormat().get(clusterType.toLowerCase());
        if (StringUtil.isEmpty(hickwallAddress)) hickwallAddress = DEFAULT_HICKWALL_CLUSTER_METRIC_FORMAT;
        return RetMessage.createSuccessMessage(String.format(hickwallAddress, clusterName));
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/default-routes/{srcDcName}", method = RequestMethod.GET)
    public List<RouteInfoModel> getClusterDefaultRoutesByClusterName(@PathVariable String srcDcName, @PathVariable String clusterName) {
        return clusterService.findClusterDefaultRoutesBySrcDcNameAndClusterName(srcDcName, clusterName);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/used-routes/{srcDcName}", method = RequestMethod.GET)
    public List<RouteInfoModel> getClusterUsedRoutesByClusterName(@PathVariable String srcDcName, @PathVariable String clusterName) {
        return clusterService.findClusterUsedRoutesBySrcDcNameAndClusterName(srcDcName, clusterName);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/designated-routes/{srcDcName}", method = RequestMethod.GET)
    public List<RouteInfoModel> getClusterDesignatedRoutesByClusterName(@PathVariable String srcDcName, @PathVariable String clusterName) {
        return clusterService.findClusterDesignateRoutesBySrcDcNameAndClusterName(srcDcName, clusterName);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/designated-routes/{srcDcName}", method = RequestMethod.POST)
    public RetMessage updateClusterDesignatedRoutesByClusterName(@PathVariable String clusterName, @PathVariable String srcDcName,
                                                                 @RequestBody(required = false) List<RouteInfoModel> newDesignatedRoutes) {
        try {
            if (newDesignatedRoutes == null) newDesignatedRoutes = new ArrayList<>();

            clusterService.updateClusterDesignateRoutes(clusterName, srcDcName, newDesignatedRoutes);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.info("[updateClusterDesignatedRoutesByClusterName] update designate routes {} of cluster:{} at dc:{} fail",
                    newDesignatedRoutes, clusterName, srcDcName, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    private void updateDcClustersByDcClusterModels(List<DcClusterModel> dcClusterModels, ClusterTbl clusterTbl) {
        if (clusterTbl == null) {
            throw new BadRequestException("[updateDcClustersByDcClusterModels] cluster can not be null!");
        }
        dcClusterService.validateDcClusters(dcClusterModels, clusterTbl);
        List<DcClusterModel> originDcClusters = dcClusterService.findRelatedDcClusterModels(clusterTbl.getId());

        updateDcClustersByDcClusterModels(originDcClusters, dcClusterModels, clusterTbl);
    }

    private void updateDcClustersByDcClusterModels(List<DcClusterModel> originDcClusters,
                                                   List<DcClusterModel> targetDcClusters, ClusterTbl clusterTbl){
        List<DcClusterModel> toCreates = (List<DcClusterModel>) setOperator.difference(DcClusterModel.class,
                targetDcClusters, originDcClusters, dcClusterModelComparator);
        List<DcClusterModel> toDeletes = (List<DcClusterModel>) setOperator.difference(DcClusterModel.class,
                originDcClusters, targetDcClusters, dcClusterModelComparator);
        List<DcClusterModel> left = (List<DcClusterModel>) setOperator.intersection(DcClusterModel.class,
                originDcClusters, targetDcClusters, dcClusterModelComparator);

        try {
            handleUpdateDcClusters(toCreates, toDeletes, left, clusterTbl);
        } catch (Exception e) {
            throw new ServerException(e.getMessage());
        }
    }

    private void handleUpdateDcClusters(List<DcClusterModel> toCreates, List<DcClusterModel> toDeletes,
                                        List<DcClusterModel> toUpdates, ClusterTbl clusterTbl) throws DalException {
        if (toDeletes != null && !toDeletes.isEmpty()) {
            logger.info("[updateDcClustersByDcClusterModels] delete dc cluster {}, {}", toDeletes.size(), toDeletes);
            deleteDcClusterBatch(toDeletes, clusterTbl);
        }

        if (toCreates != null && !toCreates.isEmpty()) {
            logger.info("[updateDcClustersByDcClusterModels] create dc cluster {}, {}", toCreates.size(), toCreates);
            createDcClusterBatch(toCreates, clusterTbl);
        }

        if (toUpdates != null && !toUpdates.isEmpty()) {
            logger.info("[updateDcClustersByDcClusterModels] update dc cluster {}, {}", toUpdates.size(), toUpdates);
            updateDcClusterBatch(toUpdates, clusterTbl);
        }
    }

    private void createDcClusterBatch(List<DcClusterModel> toCreates, ClusterTbl clusterTbl) {
        toCreates.forEach( toCreate ->{
            toCreate.getDcCluster().setDcName(toCreate.getDc().getDc_name());
            toCreate.getDcCluster().setClusterName(clusterTbl.getClusterName());
            clusterService.bindDc(toCreate.getDcCluster());

            if (!toCreate.getDcCluster().isGroupType() && toCreate.getShards() != null) {
                List<DcClusterTbl> dcClusterTbls = new ArrayList<>();
                dcClusterTbls.add(dcClusterService.find(toCreate.getDcCluster().getDcName(),
                        toCreate.getDcCluster().getClusterName()));

                ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
                toCreate.getShards().forEach(shardModel -> {
                        shardService.findOrCreateShardIfNotExist(clusterTbl.getClusterName(), shardModel.getShardTbl(),
                                dcClusterTbls, sentinelBalanceService.selectMultiDcSentinels(clusterType));
                    });
            }
        });
    }

    private void deleteDcClusterBatch(List<DcClusterModel> toDeletes, ClusterTbl clusterTbl) throws DalException {
        for (DcClusterModel toDelete : toDeletes){
            if (toDelete.getDcCluster().getDcId() == clusterTbl.getActivedcId()) {
                throw new BadRequestException("can not unbind active dc");
            }
            clusterService.unbindDc(clusterTbl.getClusterName(), dcService.find(toDelete.getDcCluster().getDcId()).getDcName());
        }
    }

    private void updateDcClusterBatch(List<DcClusterModel> toUpdates, ClusterTbl clusterTbl) throws DalException {

        for (DcClusterModel toUpdate : toUpdates){
            if (toUpdate.getDcCluster().isGroupType()
                    && !ObjectUtils.equals(toUpdate.getDcCluster().getDcId(), clusterTbl.getActivedcId())) {
                continue;
            }

            updateShardsByDcClusterModel(toUpdate, clusterTbl);
        }
    }

    private void updateShardsByDcClusterModel(DcClusterModel dcClusterModel, ClusterTbl clusterTbl) throws DalException {
        List<ShardTbl> targetShards = new ArrayList<>();
        dcClusterModel.getShards().forEach(shardModel -> {
            targetShards.add(shardModel.getShardTbl());
        });

        List<ShardTbl> originShards = shardService.findAllShardByDcCluster(dcClusterModel.getDcCluster().getDcId(),
                dcClusterModel.getDcCluster().getClusterId());
        ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
        handleShardsUpdate(targetShards, originShards, clusterTbl, clusterType, dcClusterModel.getDcCluster());
    }


    private void handleShardsUpdate(List<ShardTbl> targetShards, List<ShardTbl> originShards, ClusterTbl clusterTbl,
                                    ClusterType clusterType, DcClusterTbl dcClusterTbl) throws DalException {
        List<ShardTbl> toCreates = (List<ShardTbl>) setOperator.difference(ShardTbl.class, targetShards,
                originShards, shardTblComparator);
        List<ShardTbl> toDeletes = (List<ShardTbl>) setOperator.difference(ShardTbl.class, originShards,
                targetShards, shardTblComparator);

        List<String> toDeleteShardNames = new ArrayList<>();
        toDeletes.forEach(toDelete -> {
            toDeleteShardNames.add(toDelete.getShardName());
        });
        shardService.deleteShards(clusterTbl, toDeleteShardNames);

        List<DcClusterTbl> dcClusterTbls =
                dcClusterService.findAllByClusterAndGroupType(clusterTbl.getId(), dcClusterTbl.getDcId(), dcClusterTbl.isGroupType());
        toCreates.forEach(toCreate -> {
            shardService.findOrCreateShardIfNotExist(clusterTbl.getClusterName(), toCreate,
                    dcClusterTbls, sentinelBalanceService.selectMultiDcSentinels(clusterType));
        });
    }

    protected void updateClusterReplDirections(ClusterTbl clusterTbl, List<ReplDirectionInfoModel> replDirections) {
        if (clusterTbl == null) {
            throw new BadRequestException("[updateClusterReplDirections] cluster can not be null!");
        }
        Map<String, Long> dcNameIdMap = dcService.dcNameIdMap();

        List<ReplDirectionTbl> originReplDirections = replDirectionService.findAllReplDirectionTblsByCluster(clusterTbl.getId());
        List<ReplDirectionTbl> targetReplDirections =
                replDirectionService.convertReplDirectionInfoModelsToReplDirectionTbls(replDirections, dcNameIdMap);

        replDirectionService.validateReplDirection(clusterTbl, targetReplDirections);
        updateClusterReplDirections(originReplDirections, targetReplDirections);
    }


    private void updateClusterReplDirections(List<ReplDirectionTbl> originReplDirections,
                                             List<ReplDirectionTbl> targetReplDirections) {

        List<ReplDirectionTbl> toCreates = (List<ReplDirectionTbl>) setOperator.difference(ReplDirectionTbl.class,
                targetReplDirections, originReplDirections, replDirectionTblComparator);

        List<ReplDirectionTbl> toDeletes = (List<ReplDirectionTbl>) setOperator.difference(ReplDirectionTbl.class,
                originReplDirections, targetReplDirections, replDirectionTblComparator);

        List<ReplDirectionTbl> toUpdates = (List<ReplDirectionTbl>) setOperator.intersection(ReplDirectionTbl.class,
                originReplDirections, targetReplDirections, replDirectionTblComparator);

        try {
            handleUpdateReplDirecitons(toCreates, toDeletes, toUpdates);
        } catch (Exception e) {
            throw new ServerException(e.getMessage());
        }
    }

    private void handleUpdateReplDirecitons(List<ReplDirectionTbl> toCreates, List<ReplDirectionTbl> toDeletes,
                                            List<ReplDirectionTbl> toUpdates) {
        if (toCreates != null && !toCreates.isEmpty()) {
            logger.info("[updateClusterReplDirections] create repl direction {}", toCreates);
            replDirectionService.createReplDirectionBatch(toCreates);
        }

        if (toDeletes != null && !toDeletes.isEmpty()) {
            logger.info("[updateClusterReplDirections] delete repl direction {}", toDeletes);
            replDirectionService.deleteReplDirectionBatch(toDeletes);
            toCreates.forEach(toCreate -> {
                List<ShardTbl> allSrcDcShards = shardService.findAllShardByDcCluster(toCreate.getSrcDcId(), toCreate.getClusterId());
                if(null!=allSrcDcShards && !allSrcDcShards.isEmpty()) {
                    for (ShardTbl shardTbl : allSrcDcShards) {
                        applierService.deleteAppliers(shardTbl, toCreate.getId());
                    }
                }
            });
        }

        if (toUpdates != null && !toUpdates.isEmpty()) {
            logger.info("[updateClusterReplDirections] update repl direction {}", toUpdates);
            replDirectionService.updateReplDirectionBatch(toUpdates);
        }
    }
}
