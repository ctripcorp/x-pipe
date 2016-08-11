package com.ctrip.xpipe.redis.console.rest.consoleweb;


import com.ctrip.xpipe.redis.core.entity.ShardMeta;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @Author zhangle
 */
@RestController
@RequestMapping("console")
public class ShardController {


	@RequestMapping("/clusters/{clusterName}/dcs/{dcName}/shards")
	public List<ShardMeta> findShards(@PathVariable String clusterName, @PathVariable String dcName) {
		return null;
	}
}
