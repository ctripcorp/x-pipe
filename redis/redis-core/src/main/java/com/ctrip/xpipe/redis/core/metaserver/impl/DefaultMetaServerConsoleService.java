package com.ctrip.xpipe.redis.core.metaserver.impl;


import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaserverAddress;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author wenchao.meng
 *
 * Sep 5, 2016
 */
public class DefaultMetaServerConsoleService extends AbstractMetaService implements MetaServerConsoleService{

	private String dc;

	private String  metaServerAddress;
	private String  changeClusterPath;
	private String  changePrimaryDcCheckPath; 
	private String  makeMasterReadonlyPath;
	private String  changePrimaryDcPath;
	private String  getCurrentMasterPath;

	private MetricProxy metricProxy = MetricProxy.DEFAULT;

	protected DefaultMetaServerConsoleService(MetaserverAddress metaserverAddress, int retryTimes, int retryIntervalMilli, int connectTimeout, int soTimout) {
		super(retryTimes, retryIntervalMilli, connectTimeout, soTimout);
		initService(metaserverAddress);
	}
	
	public DefaultMetaServerConsoleService(MetaserverAddress metaServerAddress) {
		initService(metaServerAddress);
	}

	private void initService(MetaserverAddress metaServerAddress) {
		this.dc = metaServerAddress.getDcName();
		this.metaServerAddress = metaServerAddress.getAddress();
		changeClusterPath = META_SERVER_SERVICE.CLUSTER_CHANGE.getRealPath(metaServerAddress.getAddress());
		changePrimaryDcCheckPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC_CHECK.getRealPath(metaServerAddress.getAddress());
		makeMasterReadonlyPath = META_SERVER_SERVICE.MAKE_MASTER_READONLY.getRealPath(metaServerAddress.getAddress());
		changePrimaryDcPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC.getRealPath(metaServerAddress.getAddress());
		getCurrentMasterPath = META_SERVER_SERVICE.GET_CURRENT_MASTER.getRealPath(metaServerAddress.getAddress());
	}

	@Override
	public void clusterAdded(String clusterId, ClusterMeta clusterMeta) {

		doRequest("clusterAdded", clusterId, () -> restTemplate.postForEntity(changeClusterPath, clusterMeta, String.class, clusterId));
	}

	@Override
	public void clusterModified(String clusterId, ClusterMeta clusterMeta) {

		doRequest("clusterModified", clusterId, () -> restTemplate.put(changeClusterPath, clusterMeta, clusterId));
	}

	@Override
	public void clusterDeleted(String clusterId) {

		doRequest("clusterDeleted", clusterId, () -> restTemplate.delete(changeClusterPath, clusterId));
	}

	@Override
	public PrimaryDcCheckMessage changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc) {

		return doRequest("changePrimaryDcCheck", clusterId, () ->
				restTemplate.getForObject(changePrimaryDcCheckPath, PrimaryDcCheckMessage.class, clusterId, shardId, newPrimaryDc));
	}

	@Override
	public PreviousPrimaryDcMessage makeMasterReadOnly(String clusterId, String shardId, boolean readOnly) {

		return doRequest("makeMasterReadOnly", clusterId, () -> restTemplate.exchange(
				makeMasterReadonlyPath,
				HttpMethod.PUT, HttpEntity.EMPTY,
				PreviousPrimaryDcMessage.class, clusterId, shardId, readOnly).getBody());
	}

	@Override
	public PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc, PrimaryDcChangeRequest request) {

		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8_VALUE);
		HttpEntity<Object> entity = new HttpEntity<Object>(request, httpHeaders);

		return doRequest("changePrimaryDc", clusterId, () -> restTemplate.exchange(changePrimaryDcPath,
				HttpMethod.PUT,
				entity,
				PrimaryDcChangeMessage.class,
				clusterId, shardId, newPrimaryDc).getBody());
	}

	@Override
	public RedisMeta getCurrentMaster(String clusterId, String shardId) {

		return doRequest("getCurrentMaster", clusterId, () ->
				restTemplate.getForObject(getCurrentMasterPath, RedisMeta.class, clusterId, shardId));

	}

	@Override
	protected List<String> getMetaServerList() {
		
		List<String> result = new ArrayList<>();
		result.add(metaServerAddress);
		return result;
	}

	private void doRequest(String api, String cluster, Runnable request) {
		long startTime = System.currentTimeMillis();
		try {
			request.run();
			tryMetric(api, "", cluster, true, startTime, System.currentTimeMillis());
		} catch (Throwable th) {
			tryMetric(api, "", cluster, false, startTime, System.currentTimeMillis());
			throw th;
		}
	}

	private <T> T doRequest(String api, String cluster, Supplier<T> request) {
		long startTime = System.currentTimeMillis();
		T resp = null;
		try {
			resp = request.get();
			return resp;
		} finally {
			tryMetric(api, dc, cluster, null != resp, startTime, System.currentTimeMillis());
		}
	}

	private void tryMetric(String api, String dc, String cluster, boolean isSuccess, long startTime, long endTime) {
		try {
			MetricData metricData = new MetricData("call.metaserver", dc, cluster, null);
			metricData.setTimestampMilli(startTime);
			metricData.addTag("api", api);
			metricData.setValue(endTime - startTime);
			metricData.addTag("status", isSuccess ? "SUCCESS" : "FAIL");
			metricProxy.writeBinMultiDataPoint(metricData);
		} catch (Throwable th) {
			logger.debug("[tryMetric] fail", th);
		}
	}

	@VisibleForTesting
	protected void setMetricProxy(MetricProxy metricProxy) {
		this.metricProxy = metricProxy;
	}

}
