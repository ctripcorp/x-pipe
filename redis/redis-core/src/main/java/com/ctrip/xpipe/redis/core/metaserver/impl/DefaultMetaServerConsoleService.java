package com.ctrip.xpipe.redis.core.metaserver.impl;


import java.util.ArrayList;
import java.util.List;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.retry.RetryPolicyFactories;
import com.ctrip.xpipe.spring.RestTemplateFactory;

/**
 * @author wenchao.meng
 *
 * Sep 5, 2016
 */
public class DefaultMetaServerConsoleService extends AbstractMetaService implements MetaServerConsoleService{

	private int MAX_PER_ROUTE = Integer.parseInt(System.getProperty("max-per-route", "1000"));
	private int MAX_TOTAL = Integer.parseInt(System.getProperty("max-per-route", "10000"));
	private int RETRY_TIMES = Integer.parseInt(System.getProperty("retry-times", "8"));
	private int CONNECT_TIMEOUT = Integer.parseInt(System.getProperty("connect-timeout", "8000"));
	private int SO_TIMEOUT = Integer.parseInt(System.getProperty("so-timeout", "8000"));
	
	private String  metaServerAddress;
	private String  changeClusterPath;
	private String  changePrimaryDcCheckPath; 
	private String  makeMasterReadonlyPath;
	private String  changePrimaryDcPath;

	
	public DefaultMetaServerConsoleService(String metaServerAddress) {
		this.metaServerAddress = metaServerAddress;
		changeClusterPath = META_SERVER_SERVICE.CLUSTER_CHANGE.getRealPath(metaServerAddress);
		changePrimaryDcCheckPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC_CHECK.getRealPath(metaServerAddress);
		makeMasterReadonlyPath = META_SERVER_SERVICE.MAKE_MASTER_READONLY.getRealPath(metaServerAddress);
		changePrimaryDcPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC.getRealPath(metaServerAddress);
		
		this.restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(
				MAX_PER_ROUTE,
				MAX_TOTAL,
				CONNECT_TIMEOUT,
				SO_TIMEOUT,
				RETRY_TIMES,
				RetryPolicyFactories.newRestOperationsRetryPolicyFactory(5));
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
		restTemplate.put(makeMasterReadonlyPath, null, clusterId, shardId, readOnly);
		
	}

	@Override
	public PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc) {
		
		HttpEntity<Object> entity = new HttpEntity<Object>(null);
		return restTemplate.exchange(changePrimaryDcPath, HttpMethod.PUT, entity, PrimaryDcChangeMessage.class, clusterId, shardId, newPrimaryDc).getBody();
	}

	@Override
	protected List<String> getMetaServerList() {
		
		List<String> result = new ArrayList<>();
		result.add(metaServerAddress);
		return result;
	}

}
