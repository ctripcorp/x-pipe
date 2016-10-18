package com.ctrip.xpipe.redis.console.rest.consoleweb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.RedisService;

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
			@PathVariable String shardName, @RequestBody ShardModel shardModel) {
		logger.info("[Update Redises]{},{},{},{}",clusterName, dcName, shardName, shardModel);
		redisService.updateRedises(clusterName,dcName,shardName,shardModel);
	}
  
}
