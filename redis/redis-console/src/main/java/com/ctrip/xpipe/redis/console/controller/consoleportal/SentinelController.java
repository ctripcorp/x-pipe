package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *
 * Jan 4, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class SentinelController extends AbstractConsoleController{
	@Autowired
	private SentinelService sentinelService;
	
	@RequestMapping(value="/{dcName}/sentinels", method = RequestMethod.GET)
	public List<SetinelTbl> getSentinelsByDcName(@PathVariable String dcName) {
		return sentinelService.findAllByDcName(dcName);
	}
	
	@RequestMapping(value="/sentinels/{sentinelId}", method = RequestMethod.GET) 
	public SetinelTbl findSentinel(@PathVariable long sentinelId){
		return sentinelService.find(sentinelId);
	}
	
	@RequestMapping(value="/sentinels/shard/{shardId}", method = RequestMethod.GET) 
	public Map<Long,SetinelTbl> findSentinelByShard(@PathVariable long shardId) {
		return sentinelService.findByShard(shardId);
	}
}
