package com.ctrip.xpipe.redis.console.rest.consoleweb;

import com.ctrip.xpipe.redis.console.model.RedisTbl;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zhangle
 * 16/8/24
 */
@RestController
@RequestMapping("console")
public class RedisController {


  @RequestMapping(value = "/clusters/{clusterName}/dcs/{dcName}/shards/{shardName}/redises/{redisName}", method = RequestMethod.PUT)
  public void updateRedis(@PathVariable String clusterName, @PathVariable String dcName,
                          @PathVariable String shardName, @PathVariable String redisName,
                          @RequestBody RedisTbl redis) {

  }

  @RequestMapping(value = "/clusters/{clusterName}/dcs/{dcName}/shards/{shardName}/redises", method = RequestMethod.POST)
  public RedisTbl bindRedis(@PathVariable String clusterName, @PathVariable String dcName,
                            @PathVariable String shardName, @RequestBody RedisTbl redis) {
    return null;

  }

  @RequestMapping(value = "/clusters/{clusterName}/dcs/{dcName}/shards/{shardName}/redises/{redisName}", method = RequestMethod.DELETE)
  public void unbindRedis(@PathVariable String clusterName, @PathVariable String dcName,
                          @PathVariable String shardName, @PathVariable String redisName) {

  }



}
