package com.ctrip.xpipe.redis.console.rest.metaserver;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.xpipe.redis.console.service.MetaInfoService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author zhangle
 *
 */
@RestController
@RequestMapping("/api")
public class ConsoleController {
	
	@Autowired
	private MetaInfoService metaInfoService;
	
	@RequestMapping("/dc/{dcId}")
	public DcMeta getDcMeta(@PathVariable String dcId) throws DalException {
		return metaInfoService.getDcMeta(dcId);
	}
	
	@RequestMapping("/dc/{dcId}/cluster/{clusterId}")
	public ClusterMeta getDcClusterMeta(@PathVariable String dcId,@PathVariable String clusterId) throws DalException {
		return metaInfoService.getDcClusterMeta(dcId, clusterId);
	}
	
	@RequestMapping("/dc/{dcId}/cluster/{clusterId}/shard/{shardId}")
	public ShardMeta getDcClusterShardMeta(@PathVariable String dcId,@PathVariable String clusterId,
			@PathVariable String shardId) throws DalException {
		return metaInfoService.getDcClusterShardMeta(dcId, clusterId, shardId);
	}
	
	@RequestMapping("/dcids")
	public List<String> getDcIds() throws DalException {
		return metaInfoService.getAllDcIds();
	}
	
	@RequestMapping("/clusterids")
	public List<String> getClusterIds() throws DalException {
		return metaInfoService.getAllClusterIds();
	}
	
	@RequestMapping("/cluster/{clusterId}/shardids")
	public List<String> getShardIds(@PathVariable String clusterId) throws DalException {
		return metaInfoService.getAllClusterShardIds(clusterId);
	}

}
