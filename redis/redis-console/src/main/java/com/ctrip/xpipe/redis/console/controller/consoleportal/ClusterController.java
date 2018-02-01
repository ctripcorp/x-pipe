package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.ClusterListClusterModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * @author zhangle
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class ClusterController extends AbstractConsoleController {

    @Autowired
    private DcService dcService;
    @Autowired
    private ClusterService clusterService;
    @Autowired
    private DcClusterService dcClusterService;

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs", method = RequestMethod.GET)
    public List<DcTbl> findClusterDcs(@PathVariable String clusterName) {
        return valueOrEmptySet(DcTbl.class, dcService.findClusterRelatedDc(clusterName));
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public ClusterTbl loadCluster(@PathVariable String clusterName) {
        return valueOrDefault(ClusterTbl.class, clusterService.findClusterAndOrg(clusterName));
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
    public List<ClusterListClusterModel> findUnhealthyClusters() {
        return valueOrEmptySet(ClusterListClusterModel.class, clusterService.findUnhealthyClusters());
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

}
