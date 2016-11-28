package com.ctrip.xpipe.redis.core.metaserver.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.util.concurrent.ListenableFuture;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;

/**
 * @author wenchao.meng
 *
 * Sep 5, 2016
 */
public class DefaultMetaServerConsoleService extends AbstractMetaService implements MetaServerConsoleService{
	
	private String  metaServerAddress;
	private String  changeClusterPath;

	
	public DefaultMetaServerConsoleService(String metaServerAddress) {
		this.metaServerAddress = metaServerAddress;
		changeClusterPath = String.format("%s/%s/%s", metaServerAddress, MetaServerConsoleService.PATH_PREFIX, MetaServerConsoleService.PATH_CLUSTER_CHANGE);
	}

	@Override
	public void clusterAdded(String clusterId, ClusterMeta clusterMeta) {
		
		restTemplate.postForEntity(changeClusterPath, clusterMeta, String.class, clusterId);
	}

	@Override
	public void clusterModified(String clusterId, ClusterMeta clusterMeta) {
		
		restTemplate.put(changeClusterPath, clusterMeta, clusterId);
	}

	@Override
	public void clusterDeleted(String clusterId) {
		
		restTemplate.delete(changeClusterPath, clusterId);
	}


	@Override
	public DcMeta getDynamicInfo() {
		
		return null;
	}

	@Override
	protected List<String> getMetaServerList() {
		
		List<String> result = new ArrayList<>();
		result.add(metaServerAddress);
		return result;
	}

	@Override
	public ChangePrimaryDcResult changePrimaryDc(String clusterId, String shardId, String primaryDc, long eventId) {
		return null;
	}

	@Override
	public ListenableFuture<ResponseEntity<String>> getChangePrimaryDcStatus(long eventId, long offset) {
		return null;
	}

}
