package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.KeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedList;
import java.util.List;

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

  @RequestMapping(value = "/keepers/{dcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
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

  @RequestMapping(value = "/keepers/{dcId}/{clusterId}/{shardId}", method = RequestMethod.POST)
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

    try {
      List<KeeperBasicInfo> bestKeepers =
          keeperAdvancedService.findBestKeepers(dcId, RedisProtocol.REDIS_PORT_DEFAULT, (ip, port) -> true, clusterId);
      logger.info("[addKeepers]{},{},{},{}, {}", dcId, clusterId, shardId, bestKeepers);
      redisService.insertKeepers(dcId, clusterId, shardId, bestKeepers);
      return RetMessage.createSuccessMessage("insert success:" + bestKeepers);
    } catch (Exception e) {
      logger.error("[addKeepers]" + dcId + "," + clusterId + "," + shardId, e);
      return RetMessage.createFailMessage("insert fail:" + e.getMessage());
    }
  }

  @RequestMapping(value = "/keepers/{dcId}/{clusterId}/{shardId}", method = RequestMethod.DELETE)
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
}
