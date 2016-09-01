package com.ctrip.xpipe.redis.console.rest.consoleweb;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zhangle 16/8/24
 */
@RestController
@RequestMapping("console")
public class RedisController {
  @Autowired
  private RedisService redisService;
  
  // TODO
  @RequestMapping(value = "/clusters/{clusterName}/dcs/{dcName}/shards/{shardName}/redises", method = RequestMethod.POST)
  public RedisTbl bindRedis(@PathVariable String clusterName, @PathVariable String dcName,
                            @PathVariable String shardName, @RequestBody RedisTbl redis) {
    return redisService.bindRedis(clusterName, dcName, shardName, redis);

  }

  
  // TODO
  @RequestMapping(value = "/clusters/{clusterName}/dcs/{dcName}/shards/{shardName}/redises/{redisName}", method = RequestMethod.DELETE)
  public void unbindRedis(@PathVariable String clusterName, @PathVariable String dcName,
                          @PathVariable String shardName, @PathVariable String redisName) {

    redisService.deleteRedis(redisName);
  }
  
}
