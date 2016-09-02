package com.ctrip.xpipe.redis.console.rest.consoleweb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author zhangle 16/8/24
 */
@RestController
@RequestMapping("console")
public class RedisController extends AbstractConsoleController{
	@Autowired
	private RedisService redisService;
	
	@RequestMapping(value = "/clusters/{clusterName}/dcs/{dcName}/shards/{shardName}", method = RequestMethod.POST)
	public void updateRedises(@PathVariable String clusterName, @PathVariable String dcName,
			@PathVariable String shardName, @RequestBody ShardMeta shardMeta) {
		redisService.updateRedises(clusterName,dcName,shardName,shardMeta);
	}
  
}
