package com.ctrip.xpipe.redis.console.rest.metaserver;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import org.springframework.web.bind.annotation.RestController;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.console.service.meta.ShardMetaService;
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
	private static Codec coder = new JsonCodec();
	
	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterService clusterService;
	@Autowired
	private ShardService shardService;
	@Autowired
	private DcMetaService dcMetaService;
	@Autowired
	private ClusterMetaService clusterMetaService;
	@Autowired 
	private ShardMetaService shardMetaService;

	@RequestMapping(value = "/dc/{dcId}", method = RequestMethod.GET, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public String getDcMeta(@PathVariable String dcId, @RequestParam(value="format", required = false) String format) {
		DcMeta result = dcMetaService.getDcMeta(dcId);
		return (format != null && format.equals("xml"))? result.toString() : coder.encode(result);
	}
	
	@RequestMapping(value = "/dc/{dcId}/cluster/{clusterId}", method = RequestMethod.GET, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public String getDcClusterMeta(@PathVariable String dcId,@PathVariable String clusterId, @RequestParam(value="format", required = false) String format) {
		ClusterMeta result = clusterMetaService.getClusterMeta(dcId, clusterId);
		return (format != null && format.equals("xml"))? result.toString() : coder.encode(result);
	}
	
	@RequestMapping(value = "/dc/{dcId}/cluster/{clusterId}/shard/{shardId}", method = RequestMethod.GET, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public String getDcClusterShardMeta(@PathVariable String dcId,@PathVariable String clusterId,
			@PathVariable String shardId, @RequestParam(value="format", required = false) String format) {
		ShardMeta result = shardMetaService.getShardMeta(dcId, clusterId, shardId);
		return (format != null && format.equals("xml"))? result.toString() : coder.encode(result);
	}
	
	@RequestMapping(value = "/dcids", method = RequestMethod.GET)
	public List<String> getAllDcs(){
		List<String> result = new LinkedList<String>();
		
		for(DcTbl dc : dcService.findAllDcNames()) {
			result.add(dc.getDcName());
		}

		return result;
	}
	
	@RequestMapping(value = "/clusterids", method = RequestMethod.GET)
	public List<String> getAllClusters() {
		List<String> result = new LinkedList<String>();
		
		for(ClusterTbl cluster : clusterService.findAllClusterNames()) {
			result.add(cluster.getClusterName());
		}
		
		return result;
	}
	
	@RequestMapping(value = "/cluster/{clusterId}/shardids", method = RequestMethod.GET)
	public List<String> getAllShards(@PathVariable String clusterId) {
		List<String> result = new LinkedList<String>();
		
		for(ShardTbl shard : shardService.findAllShardNamesByClusterName(clusterId)) {
			result.add(shard.getShardName());
		}
		
		return result;
	}

}
