package com.ctrip.xpipe.redis.console.controller.api.metaserver;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.meta.*;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
	@Autowired
	private KeeperContainerService keeperContainerService;

	@RequestMapping(value = "/dc/{dcId}", method = RequestMethod.GET, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public byte[] getDcMeta(@PathVariable String dcId, @RequestParam(value="format", required = false) String format,
							@RequestParam(value ="types", required = false) Set<String> types) throws Exception {
		DcMeta dcMeta;
		Set<String> upperCaseTypes = types == null ? Collections.emptySet()
			: types.stream().map(String::toUpperCase).collect(Collectors.toSet());
		if (CollectionUtils.isEmpty(upperCaseTypes)) {
			dcMeta = dcMetaService.getDcMeta(dcId);
		} else {
			Set<String> searchTypes = Sets.newHashSet(upperCaseTypes);
			searchTypes.add(ClusterType.HETERO.toString());
			dcMeta = dcMetaService.getDcMeta(dcId, searchTypes);
		}
		List<String> toRemoveClusters = new LinkedList<>();
		dcMeta.getClusters().forEach((clusterName, clusterMeta) -> {
			ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
			if (clusterType != ClusterType.HETERO || StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
				return;
			}

			ClusterType azGroupClusterType = ClusterType.lookup(clusterMeta.getAzGroupType());
			if (upperCaseTypes.contains(azGroupClusterType.toString())) {
				clusterMeta.setType(azGroupClusterType.toString());
				clusterMeta.setAzGroupType(null);
			} else {
				toRemoveClusters.add(clusterName);
			}
		});
		toRemoveClusters.forEach(clusterName -> dcMeta.getClusters().remove(clusterName));
		String res = (format != null && format.equals("xml"))? dcMeta.toString() : coder.encode(dcMeta);
		return res.getBytes();
	}

	@RequestMapping(value = "/dc/{dcId}/cluster/{clusterId}", method = RequestMethod.GET, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public String getDcClusterMeta(@PathVariable String dcId,@PathVariable String clusterId, @RequestParam(value="format", required = false) String format) {
		ClusterMeta meta = clusterMetaService.getClusterMeta(dcId, clusterId);
		return (format != null && format.equals("xml"))? meta.toString() : coder.encode(meta);
	}
	
	@RequestMapping(value = "/dc/{dcId}/cluster/{clusterId}/shard/{shardId}", method = RequestMethod.GET, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public String getDcClusterShardMeta(@PathVariable String dcId,@PathVariable String clusterId,
			@PathVariable String shardId, @RequestParam(value="format", required = false) String format) {
		Map<Long, Long> keepContainerId2DcMap = keeperContainerService.keeperContainerIdDcMap();

		ShardMeta result = shardMetaService.getShardMeta(dcId, clusterId, shardId, keepContainerId2DcMap);
		if (result == null) {
		    return "";
		}
		return (format != null && format.equals("xml"))? result.toString() : coder.encode(result);
	}
	
	@RequestMapping(value = "/dcids", method = RequestMethod.GET)
	public List<String> getAllDcs(){
		return dcService.findAllDcNames();
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
