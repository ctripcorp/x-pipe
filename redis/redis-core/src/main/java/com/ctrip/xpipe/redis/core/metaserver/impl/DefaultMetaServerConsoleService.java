package com.ctrip.xpipe.redis.core.metaserver.impl;


import java.util.ArrayList;
import java.util.List;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;

/**
 * @author wenchao.meng
 *
 * Sep 5, 2016
 */
public class DefaultMetaServerConsoleService extends AbstractMetaService implements MetaServerConsoleService{
	
	private String  metaServerAddress;
	private String  changeClusterPath;
	private String  changePrimaryDcCheckPath; 

	
	public DefaultMetaServerConsoleService(String metaServerAddress) {
		this.metaServerAddress = metaServerAddress;
		changeClusterPath = META_SERVER_SERVICE.CLUSTER_CHANGE.getRealPath(metaServerAddress);
		changePrimaryDcCheckPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC_CHECK.getRealPath(metaServerAddress);
		
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
	public PrimaryDcCheckMessage changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc) {
		
		return restTemplate.getForObject(changePrimaryDcCheckPath, PrimaryDcCheckMessage.class, clusterId, shardId, newPrimaryDc);
	}

	@Override
	public void makeMasterReadOnly(String clusterId, String shardId, boolean readOnly) {
		
	}

	@Override
	public PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc) {
		return null;
	}

	@Override
	protected List<String> getMetaServerList() {
		
		List<String> result = new ArrayList<>();
		result.add(metaServerAddress);
		return result;
	}

}
