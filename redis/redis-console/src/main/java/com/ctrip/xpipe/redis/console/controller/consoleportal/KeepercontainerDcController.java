package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.KeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;


/**
 * @author shyin
 *         <p>
 *         Aug 22, 2016
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class KeepercontainerDcController extends AbstractConsoleController {

    @Autowired
    private KeepercontainerService keepercontainerService;

    @Autowired
    private KeeperAdvancedService keeperAdvancedService;

    @Autowired
    private ClusterService clusterService;


    @RequestMapping(value = "/dcs/{dcName}/cluster/{clusterName}/activekeepercontainers", method = RequestMethod.GET)
    public List<KeepercontainerTbl> findKeeperContainersByCluster(@PathVariable String dcName,
                                                                @PathVariable String clusterName) {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        long clusterOrgId = clusterTbl.getClusterOrgId();
        return keepercontainerService.findKeeperCountByClusterOrg(dcName, clusterOrgId);
    }


    @RequestMapping(value = "/dcs/{dcName}/availablekeepers", method = RequestMethod.POST)
    public List<RedisTbl> findAvailableKeepers(@PathVariable String dcName,
                                               @RequestBody(required = false) ShardModel shardModel) {

        logger.debug("[findAvailableKeepers]{}, {}", dcName, shardModel);

        int beginPort = findBeginPort(shardModel);

        long clusterOrgId = getShardClusterOrgId(shardModel);

        List<KeeperBasicInfo> bestKeepers = keeperAdvancedService.findBestKeepersByCluster(dcName, beginPort, (ip, port) -> {

            if (shardModel != null && existOnConsole(ip, port, shardModel.getKeepers())) {
                return false;
            }
            return true;
        }, clusterOrgId);

        List<RedisTbl> result = new LinkedList<>();

        bestKeepers.forEach(keeperSelected -> result.add(
                new RedisTbl().setKeepercontainerId(keeperSelected.getKeeperContainerId())
                .setRedisIp(keeperSelected.getHost())
                .setRedisPort(keeperSelected.getPort())
        ));
        return result;
    }

    private boolean existOnConsole(String keepercontainerIp, int port, List<RedisTbl> keepers) {

        for (RedisTbl redisTbl : keepers) {
            if (redisTbl.getRedisIp().equalsIgnoreCase(keepercontainerIp)
                    && redisTbl.getRedisPort() == port) {
                return true;
            }
        }
        return false;
    }

    private int findBeginPort(ShardModel shardModel) {

        int port = RedisProtocol.REDIS_PORT_DEFAULT;
        if (shardModel != null && shardModel.getRedises().size() > 0) {
            port = shardModel.getRedises().get(0).getRedisPort();
        }
        return port;
    }

    private long getShardClusterOrgId(ShardModel shardModel) {
        if(shardModel == null)  return 0L;
        long clusterId = shardModel.getShardTbl().getClusterId();
        return getClusterOrgId(clusterId);
    }

    private long getClusterOrgId(long clusterId) {
        ClusterTbl clusterTbl = clusterService.find(clusterId);
        if(clusterTbl == null) {
            throw new IllegalStateException("Cluster could not be found by Id: " + clusterId);
        }
        return clusterTbl.getClusterOrgId();
    }
}
