package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.annotation.ClusterTypeLimit;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.utils.StringUtil;
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
  private KeeperContainerService keeperContainerService;

  @Autowired
  protected ClusterService clusterService;

  @Autowired
  private CrossDcKeeperContainerAddressService keeperContainerAddressService;

  @ClusterTypeLimit(value = {ClusterType.ONE_WAY, ClusterType.HETERO})
  @RequestMapping(value = "/keepers/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.GET)
  public List<String> getKeepers(@PathVariable String dcId, @PathVariable String clusterId,
      @PathVariable String shardId) {

    logger.info("[getRedises]{},{},{}", dcId, clusterId, shardId);
    dcId = outerDcToInnerDc(dcId);

    List<String> result = new LinkedList<>();
    List<RedisTbl> keeperTbls = null;
    try {
      keeperTbls = redisService.findKeepersByDcClusterShard(dcId, clusterId, shardId);
      result = formatToIpPort(keeperTbls, dcId);
    } catch (ResourceNotFoundException e) {
      logger.error("[getKeepers]", e);
    }
    return result;
  }

  private List<String> formatToIpPort(List<RedisTbl> keeperTbls, String dc) {

    List<String> result = new LinkedList<>();
    keeperTbls.forEach(redisTbl -> result.add(String.format("%s:%d", getKeeperRealIp(redisTbl, dc), redisTbl.getRedisPort())));
    return result;
  }

  private String getKeeperRealIp(RedisTbl keeper, String dc) {
    String realIp = StringUtil.isEmpty(keeper.getHostName()) ? keeper.getRedisIp() : keeperContainerAddressService.getIpByHostname(dc, keeper.getHostName());
    return StringUtil.isEmpty(realIp) ? keeper.getHostName() : realIp;
  }

  /**
   * 简单的日志输出格式：有 IP 用 IP，没有 IP 用 hostname
   */
  private String keepersToSimpleString(List<RedisTbl> keepers) {
    List<String> result = new LinkedList<>();
    for (RedisTbl keeper : keepers) {
      String addr = StringUtil.isEmpty(keeper.getRedisIp()) 
          ? keeper.getHostName() 
          : keeper.getRedisIp();
      result.add(String.format("%s:%d", addr, keeper.getRedisPort()));
    }
    return result.toString();
  }

  @RequestMapping(value = "/keepers/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.POST)
  public RetMessage addKeepers(@PathVariable String dcId, @PathVariable String clusterId,
      @PathVariable String shardId) {

    logger.info("[addKeepers]{},{},{}, {}", dcId, clusterId, shardId);
    dcId = outerDcToInnerDc(dcId);

    try {
      List<RedisTbl> keepers = redisService.findKeepersByDcClusterShard(dcId, clusterId, shardId);
      if (keepers.size() > 0) {
        return RetMessage.createSuccessMessage("alread has keepers:" + keepersToSimpleString(keepers));
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
    } catch (Throwable th) {
      logger.error("[addKeepers]" + dcId + "," + clusterId + "," + shardId, th);
      return RetMessage.createFailMessage("insert fail:" + th.getMessage());
    }
  }

  @ClusterTypeLimit(value = {ClusterType.ONE_WAY, ClusterType.HETERO})
  @RequestMapping(value = "/keepers/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.DELETE)
  public RetMessage deleteKeepers(@PathVariable String dcId, @PathVariable String clusterId,
      @PathVariable String shardId) {

    logger.info("[deleteKeepers]{},{},{}, {}", dcId, clusterId, shardId);

    dcId = outerDcToInnerDc(dcId);

    try {
      List<RedisTbl> redisTbls = redisService.deleteKeepers(dcId, clusterId, shardId);

      String message = null;
      if (redisTbls.size() > 0) {
        message = String.format("deleted:%s", keepersToSimpleString(redisTbls));
      } else {
        message = "success, but already no keepers";
      }
      return RetMessage.createSuccessMessage(message);
    } catch (Throwable th) {
      logger.error("[deleteKeepers]", th);
      return RetMessage.createFailMessage(th.getMessage());
    }
  }

  @RequestMapping(value = "/keepercontainer", method = RequestMethod.POST)
  public RetMessage addKeeperContainer(@RequestBody KeeperContainerCreateInfo createInfo) {
    try {
      createInfo.check();
      keeperContainerService.addKeeperContainer(createInfo);
      return RetMessage.createSuccessMessage("Add KeeperContainer successfully");
    } catch (Throwable th) {
      return RetMessage.createFailMessage(th.getMessage());
    }
  }

  @RequestMapping(value = "/keepercontainer/{dcName}", method = RequestMethod.GET)
  public List<KeeperContainerCreateInfo> getKeeperContainersByDc(@PathVariable String dcName) {
    try {
      return keeperContainerService.getDcAllKeeperContainers(dcName);
    } catch (Throwable th) {
      logger.error("[getKeeperContainersByDc]", th);
    }
    return Collections.emptyList();
  }

  @RequestMapping(value = "/keepercontainer", method = RequestMethod.PUT)
  public RetMessage updateKeeperContainer(@RequestBody KeeperContainerCreateInfo createInfo) {
    try {
      createInfo.check();
      keeperContainerService.updateKeeperContainer(createInfo);
      return RetMessage.createSuccessMessage();
    } catch (Throwable th) {
      logger.error("[updateKeeperContainer]", th);
      return RetMessage.createFailMessage(th.getMessage());
    }
  }

  @RequestMapping(value = "/keepercontainer/{keepercontaierIp}/{keepercontainerPort}/{keepercontainerHostName}", method = RequestMethod.DELETE)
  public RetMessage deleteKeepercontainer(@PathVariable String keepercontaierIp, @PathVariable Integer keepercontainerPort, @PathVariable(required = false) String keepercontainerHostName) {
    try {
      if (StringUtil.isEmpty(keepercontainerHostName))
        keeperContainerService.deleteKeeperContainer(keepercontaierIp, keepercontainerPort);
      else
        keeperContainerService.deleteKeeperContainerByHostname(keepercontainerHostName, keepercontainerPort);
      return RetMessage.createSuccessMessage();
    } catch (Throwable th) {
      logger.error("[deleteKeepercontainer][fail] {}:{}", keepercontaierIp, keepercontainerPort, th);
      return RetMessage.createFailMessage(th.getMessage());
    }
  }

  @RequestMapping(value = "/keeper_container/all", method = RequestMethod.GET)
  public List<KeepercontainerTbl> getAllKeeperContainers() {
    return keeperContainerService.findAll();
  }

}
