package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitorManager;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthState;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.ClusterListUnhealthyClusterModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
        return clusterService.createCluster(cluster);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.PUT)
    public void updateCluster(@PathVariable String clusterName, @RequestBody ClusterTbl cluster) {
        logger.info("[Update Cluster]{},{}", clusterName, cluster);
        clusterService.updateCluster(clusterName, cluster);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.DELETE)
    public void deleteCluster(@PathVariable String clusterName) {
        logger.info("[Delete Cluster]{}", clusterName);
        clusterService.deleteCluster(clusterName);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}", method = RequestMethod.POST)
    public void bindDc(@PathVariable String clusterName, @PathVariable String dcName) {
        logger.info("[bindDc]{},{}", clusterName, dcName);
        clusterService.bindDc(clusterName, dcName);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}", method = RequestMethod.DELETE)
    public void unbindDc(@PathVariable String clusterName, @PathVariable String dcName) {
        logger.info("[unbindDc]{}, {}", clusterName, dcName);
        clusterService.unbindDc(clusterName, dcName);
    }

    @RequestMapping(value = "/clusters/allBind/{dcName}", method = RequestMethod.GET)
    public List<ClusterTbl> findClustersByDcNameBind(@PathVariable String dcName){
        logger.info("[findClustersByDcId]dcName: {}", dcName);
        return clusterService.findAllClusterByDcNameBind(dcName);
    }

    @RequestMapping(value = "/clusters/allBind/{dcName}/{clusterType}", method = RequestMethod.GET)
    public List<ClusterTbl> findClustersByDcNameBindAndType(@PathVariable String dcName, @PathVariable String clusterType) {
        logger.info("[findClustersByDcNameBindAndType]dcName: {}, clusterType: {}", dcName, clusterType);
        return clusterService.findAllClusterByDcNameBindAndType(dcName, clusterType);
    }

    @RequestMapping(value = "/clusters/activeDc/{dcName}", method = RequestMethod.GET)
    public List<ClusterTbl> findClustersByActiveDcName(@PathVariable String dcName){
        logger.info("[findClustersByActiveDcName]dcName: {}", dcName);
        return clusterService.findActiveClustersByDcName(dcName);
    }

    @RequestMapping(value = "/clusters/activeDc/{dcName}/{clusterType}", method = RequestMethod.GET)
    public List<ClusterTbl> findClustersByActiveDcNameAndType(@PathVariable String dcName, @PathVariable String clusterType) {
        logger.info("[findClustersByActiveDcNameAndType]dcName: {}, clusterType: {}", dcName, clusterType);
        return clusterService.findActiveClustersByDcNameAndType(dcName, clusterType);
    }

    @RequestMapping(value = "/clusters/master/unhealthy/{level}", method = RequestMethod.GET)
    public Set<String> getMasterUnhealthyClusters(@PathVariable  String level) {
        logger.info("[getMasterUnhealthyClusters]level: {}", level);
        ClusterHealthState state = null;
        try {
            state = ClusterHealthState.valueOf(level.toUpperCase());
        } catch (Exception e) {
            logger.error("[getMasterUnhealthyClusters] level not matched: {}", level);
        }
        if(state != null) {
            return clusterHealthMonitorManager.getWarningClusters(state);
        }
        return Sets.newHashSet();
    }

    @RequestMapping(value = "/clusters/keepercontainer/{containerId}", method = RequestMethod.GET)
    public List<ClusterTbl> findClusterByKeeperContainer(@PathVariable Long containerId) {
        if (null == containerId || containerId <= 0) return Collections.emptyList();
        return clusterService.findAllClusterByKeeperContainer(containerId);
    }

    @RequestMapping(value = "/cluster/hickwall/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public RetMessage getClusterHickwallUrl(@PathVariable String clusterName) {
        return RetMessage.createSuccessMessage(String.format(config.getHickwallClusterMetricFormat(), clusterName));
    }

    @RequestMapping(value = "/cluster/used/routes/dc/{dcName}/cluster/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.PUT)
    public List<RouteInfoModel>  getClusterUsedRoutesByClusterName(@PathVariable String dcName, @PathVariable String clusterName) {
        logger.info("[getClusterUsedRoutesByClusterName] dcName:{}, cluster:{}",dcName, clusterName);
        try {

            return null;
        } catch (Throwable th) {
            logger.error("[getClusterUsedRoutesByClusterName]  dcName:{}, cluster:{}",dcName, clusterName, th);
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/cluster/designated/routes/dc/{dcName}/cluster/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public List<RouteInfoModel> getClusterDesignatedRoutesByClusterName(@PathVariable String dcName, @PathVariable String clusterName) {
        logger.info("[getClusterDesignatedRoutesByClusterName] dcName:{}, cluster:{}",dcName, clusterName);
        try {
            return clusterService.findClusterDesignateRoutesByDcNameAndClusterName(dcName, clusterName);
        } catch (Throwable th) {
            logger.error("[getClusterDesignatedRoutesByClusterName] dcName:{}, cluster:{}",dcName, clusterName, th);
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/cluster/designated/routes/cluster/" + CLUSTER_NAME_PATH_VARIABLE + "/route/{routeId}", method = RequestMethod.POST)
    public RetMessage addClusterDesignatedRoutesByClusterName(@PathVariable String clusterName, @PathVariable long routeId) {
        logger.info("[addClusterDesignatedRoutesByClusterName] cluster:{}, routeId:{}", clusterName, routeId);
        try {
            clusterService.addClusterDesignateRoute(clusterName, routeId);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[addClusterDesignatedRoutesByClusterName] cluster:{}, routeId:{}", clusterName, routeId, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/cluster/designated/routes/cluster/" + CLUSTER_NAME_PATH_VARIABLE + "/route/{routeId}", method = RequestMethod.DELETE)
    public RetMessage deleteClusterDesignatedRoutesByClusterName(@PathVariable String clusterName, @PathVariable long routeId) {
        logger.info("[deleteClusterDesignatedRoutesByClusterName] cluster:{}, routeId:{}", clusterName, routeId);
        try {
            clusterService.deleteClusterDesignateRoute(clusterName, routeId);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[deleteClusterDesignatedRoutesByClusterName] cluster:{}, routeId:{}", clusterName, routeId, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/cluster/designated/routes/cluster/" + CLUSTER_NAME_PATH_VARIABLE + "/route/{oldRouteId}/{newRouteId}", method = RequestMethod.PUT)
    public RetMessage updateClusterDesignatedRoutesByClusterName(@PathVariable String clusterName, @PathVariable long oldRouteId, @PathVariable long newRouteId) {
        logger.info("[updateClusterDesignatedRoutesByClusterName] cluster:{}, oldRouteId:{}, newRouteId:{}", clusterName, oldRouteId, newRouteId);
        try {
            clusterService.updateClusterDesignateRoute(clusterName, oldRouteId, newRouteId);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateClusterDesignatedRoutesByClusterName] cluster:{},  oldRouteId:{}, newRouteId:{}", clusterName, oldRouteId, newRouteId, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

}
