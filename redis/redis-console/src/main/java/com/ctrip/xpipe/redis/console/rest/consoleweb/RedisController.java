package com.ctrip.xpipe.redis.console.rest.consoleweb;

import com.google.common.base.Strings;

import com.ctrip.xpipe.redis.console.enums.RedisRole;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.RedisService;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


import java.util.Date;

/**
 * @author zhangle 16/8/24
 */
@RestController
@RequestMapping("console")
public class RedisController {
  @Autowired
  private RedisService redisService;
  @Autowired
  private DcClusterShardService dcClusterShardService;


  @RequestMapping(value = "/clusters/{clusterName}/dcs/{dcName}/shards/{shardName}/redises", method = RequestMethod.POST)
  public RedisTbl bindRedis(@PathVariable String clusterName, @PathVariable String dcName,
                            @PathVariable String shardName, @RequestBody RedisTbl redis) {
    if (redis.getRedisPort() <= 0 || Strings.isNullOrEmpty(redis.getRedisIp())
        || !isValidRedisRole(redis.getRedisRole())) {
      throw new BadRequestException("request payload error");
    }

    DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.load(dcName, clusterName, shardName);
    redis.setDcClusterShardId(dcClusterShardTbl.getDcClusterShardId());

    redis.setRedisName(String.valueOf(new Date().getTime()));
    return redisService.createRedis(redis);

  }

  @RequestMapping(value = "/clusters/{clusterName}/dcs/{dcName}/shards/{shardName}/redises/{redisName}", method = RequestMethod.DELETE)
  public void unbindRedis(@PathVariable String clusterName, @PathVariable String dcName,
                          @PathVariable String shardName, @PathVariable String redisName) {

    redisService.deleteRedis(redisName);
  }


  private boolean isValidRedisRole(String type) {
    return RedisRole.KEEPER.getValue().equals(type) || RedisRole.REDIS.getValue().equals(type);
  }

}
