package com.ctrip.xpipe.redis.meta.server.service.console;

import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author zhangle
 *
 */
@Service
public class ConsoleServiceImpl extends AbstractService implements ConsoleService {

	@Autowired
	private MetaServerConfig config;

	private String host;

	@PostConstruct
	public void init(){
		host = config.getConsoleAddress();
		logger.debug("[init][console address]{}", host);
	}

	@Override
	public Set<String> getAllDcIds() {
		String[] dcIds = restTemplate.getForObject(host + "/api/dcids", String[].class);
		if (dcIds == null || dcIds.length == 0){
			return Collections.emptySet();
		}
		return new HashSet<>(Arrays.asList(dcIds));

	}

	@Override
	public Set<String> getAllClusterIds() {
		String[] clustersIds = restTemplate.getForObject(host + "/api/clusterids", String[].class);
		if (clustersIds == null || clustersIds.length == 0){
			return Collections.emptySet();
		}
		return new HashSet<>(Arrays.asList(clustersIds));
	}

	@Override
	public Set<String> getClusterShardIds(String clusterId) {
		String[] shardIds = restTemplate.getForObject(host + "/api/cluster/{clusterId}/shardids", String[].class, clusterId);
		if (shardIds == null || shardIds.length == 0){
			return Collections.emptySet();
		}
		return new HashSet<>(Arrays.asList(shardIds));
	}

	@Override
	public DcMeta getDcMeta(String dcId, Set<String> types) {
		if (null == types) {
			return restTemplate.getForObject(host + "/api/dc/{dcId}", DcMeta.class, dcId);
		}

		UriComponents comp = UriComponentsBuilder.fromHttpUrl(host + "/api/dc/{dcId}")
				.queryParam("types", types.toArray())
				.buildAndExpand(dcId);

		return restTemplate.getForObject(comp.toString(), DcMeta.class);
	}

	@Override
	public ClusterMeta getClusterMeta(String dcId, String clusterId) {
		return restTemplate.getForObject(host + "/api/dc/{dcId}/cluster/{clusterId}", ClusterMeta.class, dcId, clusterId);
	}

	@Override
	public ShardMeta getShardMeta(String dcId, String clusterId, String shardId) {
		return restTemplate
			.getForObject(host + "/api/dc/{dcId}/cluster/{clusterId}/shard/{shardId}", ShardMeta.class, dcId, clusterId, shardId);
	}

	@Override
	public void keeperActiveChanged(String dc, String clusterId, String shardId, KeeperMeta newActiveKeeper) {
		restTemplate.put(host + "/api/dc/{dcId}/cluster/{clusterId}/shard/{shardId}/keepers/adjustment", newActiveKeeper, dc, clusterId, shardId);

	}

	@Override
	public void applierActiveChanged(String dc, String clusterId, String shardId, ApplierMeta newActiveApplier) {
		restTemplate.put(host + "/api/dc/{dcId}/cluster/{clusterId}/shard/{shardId}/appliers/adjustment", newActiveApplier, dc, clusterId, shardId);
	}

	@Override
	public void redisMasterChanged(String dc, String clusterId, String shardId, RedisMeta newRedisMaster) {

	}

	public void setHost(String host) {
		this.host = host;
	}
}
