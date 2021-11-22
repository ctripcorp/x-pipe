package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.annotation.ClusterTypeLimit;
import com.ctrip.xpipe.redis.checker.controller.result.GenericRetMessage;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.KEEPER_PORT_DEFAULT;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 06, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class KeeperUpdateController extends AbstractConsoleController {

  @Autowired
  private RedisService redisService;

  @Autowired
  private KeeperAdvancedService keeperAdvancedService;

  @Autowired
  private KeeperService keeperService;

  @Autowired
  private KeeperContainerService keeperContainerService;

  @Autowired
  protected ClusterService clusterService;

  @ClusterTypeLimit
  @RequestMapping(value = "/keepers/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.GET)
  public List<String> getKeepers(@PathVariable String dcId, @PathVariable String clusterId,
      @PathVariable String shardId) {

    logger.info("[getRedises]{},{},{}", dcId, clusterId, shardId);
    dcId = outerDcToInnerDc(dcId);

    List<String> result = new LinkedList<>();
    List<RedisTbl> keeperTbls = null;
    try {
      keeperTbls = redisService.findKeepersByDcClusterShard(dcId, clusterId, shardId);
      result = formatToIpPort(keeperTbls);
    } catch (ResourceNotFoundException e) {
      logger.error("[getKeepers]", e);
    }
    return result;
  }

  private List<String> formatToIpPort(List<RedisTbl> keeperTbls) {

    List<String> result = new LinkedList<>();
    keeperTbls.forEach(redisTbl -> result.add(String.format("%s:%d", redisTbl.getRedisIp(), redisTbl.getRedisPort())));
    return result;
  }

  @RequestMapping(value = "/keepers/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.POST)
  public RetMessage addKeepers(@PathVariable String dcId, @PathVariable String clusterId,
      @PathVariable String shardId) {

    logger.info("[addKeepers]{},{},{}, {}", dcId, clusterId, shardId);
    dcId = outerDcToInnerDc(dcId);

    try {
      List<RedisTbl> keepers = redisService.findKeepersByDcClusterShard(dcId, clusterId, shardId);
      if (keepers.size() > 0) {
        return RetMessage.createSuccessMessage("alread has keepers:" + formatToIpPort(keepers));
      }
    } catch (ResourceNotFoundException e) {
      logger.info("[addKeepers][not found]{}, {}, {}", dcId, clusterId, shardId);
      return RetMessage.createFailMessage(e.getMessage());
    }

    ClusterTbl clusterTbl = clusterService.find(clusterId);
    if (null == clusterTbl) {
      return RetMessage.createFailMessage("not found cluster " + clusterId);
    }
    if (!ClusterType.lookup(clusterTbl.getClusterType()).supportKeeper()) {
      return RetMessage.createFailMessage("cluster " + clusterId + " not support keepers");
    }

    try {
      List<KeeperBasicInfo> bestKeepers =
          keeperAdvancedService.findBestKeepers(dcId, KEEPER_PORT_DEFAULT, (ip, port) -> true, clusterId);
      logger.info("[addKeepers]{},{},{},{}", dcId, clusterId, shardId, bestKeepers);
      redisService.insertKeepers(dcId, clusterId, shardId, bestKeepers);
      return RetMessage.createSuccessMessage("insert success:" + bestKeepers);
    } catch (Exception e) {
      logger.error("[addKeepers]" + dcId + "," + clusterId + "," + shardId, e);
      return RetMessage.createFailMessage("insert fail:" + e.getMessage());
    }
  }

  @ClusterTypeLimit
  @RequestMapping(value = "/keepers/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.DELETE)
  public RetMessage deleteKeepers(@PathVariable String dcId, @PathVariable String clusterId,
      @PathVariable String shardId) {

    logger.info("[deleteKeepers]{},{},{}, {}", dcId, clusterId, shardId);

    dcId = outerDcToInnerDc(dcId);

    try {
      List<RedisTbl> redisTbls = redisService.deleteKeepers(dcId, clusterId, shardId);

      String message = null;
      if (redisTbls.size() > 0) {
        message = String.format("deleted:%s", formatToIpPort(redisTbls).toString());
      } else {
        message = "success, but already no keepers";
      }
      return RetMessage.createSuccessMessage(message);
    } catch (Exception e) {
      logger.error("[deleteKeepers]", e);
      return RetMessage.createFailMessage(e.getMessage());
    }
  }

  @RequestMapping(value = "/keepers/check", method = RequestMethod.POST)
  public RetMessage isKeeper(@RequestBody HostPort hostPort) {
    logger.debug("[isKeeper] check {} keeper or not", hostPort);
    try {
        boolean result = keeperService.isKeeper(hostPort);
        return GenericRetMessage.createGenericRetMessage(result);
    } catch (Exception e) {
        logger.error("[isKeeper]{}", e);
        return RetMessage.createFailMessage(e.getMessage());
    }
  }

  @RequestMapping(value = "/keepercontainer", method = RequestMethod.POST)
  public RetMessage addKeeperContainer(@RequestBody KeeperContainerCreateInfo createInfo) {
    try {
      createInfo.check();
      keeperContainerService.addKeeperContainer(createInfo);
      return RetMessage.createSuccessMessage("Add KeeperContainer successfully");
    } catch (Exception e) {
      return RetMessage.createFailMessage(e.getMessage());
    }
  }

  @RequestMapping(value = "/keepercontainer/{dcName}", method = RequestMethod.GET)
  public List<KeeperContainerCreateInfo> getKeeperContainersByDc(@PathVariable String dcName) {
    try {
      return keeperContainerService.getDcAllKeeperContainers(dcName);
    } catch (Exception e) {
      logger.error("[getKeeperContainersByDc]", e);
    }
    return Collections.emptyList();
  }

  @RequestMapping(value = "/keepercontainer", method = RequestMethod.PUT)
  public RetMessage updateKeeperContainer(@RequestBody KeeperContainerCreateInfo createInfo) {
    try {
      createInfo.check();
      keeperContainerService.updateKeeperContainer(createInfo);
      return RetMessage.createSuccessMessage();
    } catch (Exception e) {
      logger.error("[updateKeeperContainer]", e);
      return RetMessage.createFailMessage(e.getMessage());
    }
  }

  @RequestMapping(value = "/keepercontainer/{keepercontaierIp}/{keepercontainerPort}", method = RequestMethod.DELETE)
  public RetMessage deleteKeepercontainer(@PathVariable String keepercontaierIp, @PathVariable Integer keepercontainerPort) {
    try {
      keeperContainerService.deleteKeeperContainer(keepercontaierIp,keepercontainerPort);
      return RetMessage.createSuccessMessage();
    } catch (Exception e) {
      logger.error("[deleteKeepercontainer] {}:{}", keepercontaierIp, keepercontainerPort);
      return RetMessage.createFailMessage(e.getMessage());
    }
  }

}
