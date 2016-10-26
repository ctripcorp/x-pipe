package com.ctrip.xpipe.redis.console.spring;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.spring.AbstractController;

/**
 * @author shyin
 *
 * Oct 28, 2016
 */
@RestController(MetaServerConsoleService.PATH_PREFIX)
public class TestConsoleMetaController extends AbstractController {
	@RequestMapping
	public String hello() {
		return "Hello";
	}
	
	@RequestMapping(value = MetaServerConsoleService.PATH_CLUSTER_CHANGE, method = RequestMethod.POST)
	public String clusterAdd(@PathVariable String clusterId, ClusterMeta clusterMeta) {
		logger.info("[Test][ClusterAdd]{}, {}",clusterId, clusterMeta);
		return "cluster add";
	}
	
	@RequestMapping(value = MetaServerConsoleService.PATH_CLUSTER_CHANGE, method = RequestMethod.PUT)
	public String clusterModified(@PathVariable String clusterId, ClusterMeta clusterMeta) {
		logger.info("[Test][ClusterModified]{}, {}",clusterId, clusterMeta);
		return "cluster modified";
	}
	
	@RequestMapping(value = MetaServerConsoleService.PATH_CLUSTER_CHANGE, method = RequestMethod.DELETE)
	public String clusterDelete(@PathVariable String clusterId) {
		logger.info("[Test][ClusterDelete]{}", clusterId);
		return "cluster delete";
	}
	
	@RequestMapping(value = MetaServerConsoleService.PATH_UPSTREAM_CHANGE, method = RequestMethod.PUT)
	public String clusterUpstreamChanged(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable String ip, @PathVariable int port) {
		logger.info("[Test][UpstreamChanged]{}, {}, {}, {}",clusterId, shardId, ip, port);
		return "upstream changed";
	}
}
