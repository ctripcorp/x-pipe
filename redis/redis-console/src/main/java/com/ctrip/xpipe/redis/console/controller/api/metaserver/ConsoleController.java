package com.ctrip.xpipe.redis.console.controller.api.metaserver;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.meta.*;
import com.ctrip.xpipe.redis.core.entity.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author zhangle
 *
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class ConsoleController extends AbstractConsoleController {
	
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
	@Autowired
	private RedisMetaService redisMetaService;
	@Autowired
	private ApplierMetaService applierMetaService;

	@RequestMapping(value = "/dc/{dcId}", method = RequestMethod.GET, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public String getDcMeta(@PathVariable String dcId, @RequestParam(value="format", required = false) String format,
							@RequestParam(value ="types", required = false) Set<String> types) {
		DcMeta result;
		if (null != types && !types.isEmpty()) {
			result = dcMetaService.getDcMeta(dcId, types);
		} else {
			result = dcMetaService.getDcMeta(dcId);
		}

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
		
		if(null != dcService.findAllDcNames()) {
			for(DcTbl dc : dcService.findAllDcNames()) {
				result.add(dc.getDcName());
			}
		}

		return result;
	}
	
	@RequestMapping(value = "/clusterids", method = RequestMethod.GET)
	public List<String> getAllClusters() {

		return clusterService.findAllClusterNames();
	}
	
	@RequestMapping(value = "/cluster/{clusterId}/shardids", method = RequestMethod.GET)
	public List<String> getAllShards(@PathVariable String clusterId) {
		List<String> result = new LinkedList<String>();
		
		if(null != shardService.findAllShardNamesByClusterName(clusterId)) {
			for(ShardTbl shard : shardService.findAllShardNamesByClusterName(clusterId)) {
				result.add(shard.getShardName());
			}
		}
		
		return result;
	}

	@RequestMapping(value = "/dc/{dcId}/cluster/{clusterId}/shard/{shardId}/keepers/adjustment", method = RequestMethod.PUT)
	public void updateKeeperStatus(@PathVariable String dcId, @PathVariable String clusterId,
								   @PathVariable String shardId, @RequestBody(required = false) KeeperMeta newActiveKeeper){
		try {
			if(null != newActiveKeeper) {
				logger.info("[updateKeeperStatus][construct]dc:{} cluster:{} shard:{} newActiveKeeper:{}",dcId, clusterId, shardId, newActiveKeeper);
				redisMetaService.updateKeeperStatus(dcId, clusterId, shardId, newActiveKeeper);
				logger.info("[updateKeeperStatus][success]dc:{} cluster:{} shard:{} newActiveKeeper:{}",dcId, clusterId, shardId, newActiveKeeper);
			} else {
				logger.error("[updateKeeperStatus][Null Active Keeper]dc:{} cluster:{} shard:{}",dcId,clusterId,shardId);
			}
		} catch (Exception e) {
			logger.error("[updateKeeperStatus][failed]dc:{} cluster:{} shard:{} newActiveKeeper:{} Exception:{}",dcId, clusterId, shardId, newActiveKeeper,e);
		}
	}

	@RequestMapping(value = "/dc/{dcId}/cluster/{clusterId}/shard/{shardId}/appliers/adjustment", method = RequestMethod.PUT)
	public void updateApplierStatus(@PathVariable String dcId, @PathVariable String clusterId,
									@PathVariable String shardId, @RequestBody(required = false) ApplierMeta newActiveApplier){
		try {
			if(null != newActiveApplier) {
				logger.info("[updateApplierStatus][construct]dc:{} cluster:{} shard:{} newActiveApplier:{}", dcId, clusterId, shardId, newActiveApplier);
				applierMetaService.updateApplierStatus(dcId, clusterId, shardId, newActiveApplier);
				logger.info("[updateApplierStatus][success]dc:{} cluster:{} shard:{} newActiveApplier:{}", dcId, clusterId, shardId, newActiveApplier);
			} else {
				logger.error("[updateApplierStatus][Null Active Applier]dc:{} cluster:{} shard:{}", dcId, clusterId, shardId);
			}
		} catch (Exception e) {
			logger.error("[updateApplierStatus][failed]dc:{} cluster:{} shard:{} newActiveApplier:{} Exception:{}", dcId, clusterId, shardId, newActiveApplier,e);
		}
	}

}
