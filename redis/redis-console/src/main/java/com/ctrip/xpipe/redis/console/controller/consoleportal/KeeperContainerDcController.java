package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.KeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.KEEPER_PORT_DEFAULT;


/**
 * @author shyin
 *         <p>
 *         Aug 22, 2016
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class KeeperContainerDcController extends AbstractConsoleController {

  @Autowired
  private KeeperContainerService keeperContainerService;

  @Autowired
  private KeeperAdvancedService keeperAdvancedService;

  @Autowired
  private ClusterService clusterService;

  @RequestMapping(value = "/dcs/{dcName}/cluster/{clusterName}/activekeepercontainers", method = RequestMethod.GET)
  public List<KeepercontainerTbl> findKeeperContainersByCluster(@PathVariable String dcName,
      @PathVariable String clusterName) {

    List<KeepercontainerTbl> keepercontainerTbls =
         keeperContainerService.findBestKeeperContainersByDcCluster(dcName, clusterName);
    return keepercontainerTbls;
  }


  @RequestMapping(value = "/dcs/{dcName}/availablekeepers", method = RequestMethod.POST)
  public List<RedisTbl> findAvailableKeepers(@PathVariable String dcName,
      @RequestBody(required = false) ShardModel shardModel) {

    logger.debug("[findAvailableKeepers]{}, {}", dcName, shardModel);

    String clusterName = getShardClusterName(shardModel);

    List<KeeperBasicInfo> bestKeepers =
        keeperAdvancedService.findBestKeepers(dcName, KEEPER_PORT_DEFAULT, (ip, port) -> {

          if (shardModel != null && existOnConsole(ip, port, shardModel.getKeepers())) {
            return false;
          }
          return true;
        }, clusterName);

    List<RedisTbl> result = new LinkedList<>();

    bestKeepers
        .forEach(keeperSelected -> result.add(new RedisTbl().setKeepercontainerId(keeperSelected.getKeeperContainerId())
            .setRedisIp(keeperSelected.getHost()).setRedisPort(keeperSelected.getPort())));
    return result;
  }

  private boolean existOnConsole(String keepercontainerIp, int port, List<RedisTbl> keepers) {

    for (RedisTbl redisTbl : keepers) {
      if (redisTbl.getRedisIp().equalsIgnoreCase(keepercontainerIp) && redisTbl.getRedisPort() == port) {
        return true;
      }
    }
    return false;
  }

  private String getShardClusterName(ShardModel shardModel) {
    logger.debug("[getShardClusterName] shardModel: {}", shardModel);
    if (shardModel == null)
      return null;
    long clusterId = shardModel.getShardTbl().getClusterId();
    return getClusterNameById(clusterId);
  }

  private String getClusterNameById(long clusterId) {
    logger.debug("[getClusterNameById] clusterId: {}", clusterId);
    ClusterTbl clusterTbl = clusterService.find(clusterId);
    if (clusterTbl == null) {
      return null;
    }
    return clusterTbl.getClusterName();
  }
}
