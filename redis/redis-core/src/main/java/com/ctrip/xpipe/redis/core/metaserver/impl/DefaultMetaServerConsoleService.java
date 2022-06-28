package com.ctrip.xpipe.redis.core.metaserver.impl;


import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Sep 5, 2016
 */
public class DefaultMetaServerConsoleService extends AbstractMetaService implements MetaServerConsoleService{


	private String  metaServerAddress;
	private String  changeClusterPath;
	private String  changePrimaryDcCheckPath; 
	private String  makeMasterReadonlyPath;
	private String  changePrimaryDcPath;
	private String  getCurrentMasterPath;

	protected DefaultMetaServerConsoleService(String metaServerAddress, int retryTimes, int retryIntervalMilli, int connectTimeout, int soTimout) {
		super(retryTimes, retryIntervalMilli, connectTimeout, soTimout);
		initService(metaServerAddress);
	}
	
	public DefaultMetaServerConsoleService(String metaServerAddress) {
		initService(metaServerAddress);
	}

	private void initService(String metaServerAddress) {
		this.metaServerAddress = metaServerAddress;
		changeClusterPath = META_SERVER_SERVICE.CLUSTER_CHANGE.getRealPath(metaServerAddress);
		changePrimaryDcCheckPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC_CHECK.getRealPath(metaServerAddress);
		makeMasterReadonlyPath = META_SERVER_SERVICE.MAKE_MASTER_READONLY.getRealPath(metaServerAddress);
		changePrimaryDcPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC.getRealPath(metaServerAddress);
		getCurrentMasterPath = META_SERVER_SERVICE.GET_CURRENT_MASTER.getRealPath(metaServerAddress);
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
	public PreviousPrimaryDcMessage makeMasterReadOnly(String clusterId, String shardId, boolean readOnly) {

		return restTemplate.exchange(
				makeMasterReadonlyPath,
				HttpMethod.PUT, HttpEntity.EMPTY,
				PreviousPrimaryDcMessage.class, clusterId, shardId, readOnly).getBody();
	}

	@Override
	public PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc, PrimaryDcChangeRequest request) {

		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8_VALUE);
		HttpEntity<Object> entity = new HttpEntity<Object>(request, httpHeaders);

		return restTemplate.exchange(changePrimaryDcPath,
				HttpMethod.PUT,
				entity,
				PrimaryDcChangeMessage.class,
				clusterId, shardId, newPrimaryDc).getBody();
	}

	@Override
	public RedisMeta getCurrentMaster(String clusterId, String shardId) {

		return restTemplate.getForObject(getCurrentMasterPath, RedisMeta.class, clusterId, shardId);

	}

	@Override
	protected List<String> getMetaServerList() {
		
		List<String> result = new ArrayList<>();
		result.add(metaServerAddress);
		return result;
	}

}
