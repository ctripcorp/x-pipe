package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.migration.DC_TRANSFORM_DIRECTION;
import com.ctrip.xpipe.api.migration.DcMapper;
import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.migration.AbstractOuterClientService;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.monitor.CatTransactionMonitor;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public class CRedisService extends AbstractOuterClientService {

	RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(3, 100);

	private CatTransactionMonitor catTransactionMonitor = new CatTransactionMonitor();

	private CRedisConfig credisConfig = CRedisConfig.INSTANCE;

	private MetricProxy metricProxy = MetricProxy.DEFAULT;

	private final String TYPE = "credis";

	private static final ParameterizedTypeReference<List<ClusterInfo>> clustersRespTypeDef =
			new ParameterizedTypeReference<List<ClusterInfo>>(){};
	private static final ParameterizedTypeReference<OuterClientDataResp<List<ClusterExcludedIdcInfo>>> excludedIdcInfosRespTypeDef =
			new ParameterizedTypeReference<OuterClientDataResp<List<ClusterExcludedIdcInfo>>>(){};

	@Override
	public int getOrder() {
		return HIGHEST_PRECEDENCE;
	}

	@Override
	public String serviceName() {
		return "CRedis";
	}

	@Override
	public void markInstanceUp(ClusterShardHostPort clusterShardHostPort) throws OuterClientException {
		doMarkInstance(clusterShardHostPort, true);
	}

	@Override
	public void markInstanceUpIfNoModifyFor(ClusterShardHostPort clusterShardHostPort, long noModifySeconds) throws OuterClientException {
		doMarkInstanceIfNoModifyFor(clusterShardHostPort, true, noModifySeconds);
	}

	@Override
	public boolean isInstanceUp(ClusterShardHostPort clusterShardHostPort) throws OuterClientException {

		GetInstanceResult instance = getInstance(clusterShardHostPort);

		if(instance.isSuccess()){
			return instance.getResult().isCanRead();
		}
		throw new IllegalStateException("[isInstanceUp]" + clusterShardHostPort + "," + instance.getMessage());
	}

	@Override
	public Map<HostPort, Boolean> batchQueryInstanceStatus(String cluster, Set<HostPort> instances) throws OuterClientException {
		CRedisGeneralResp<List<InstanceStatus>> credisResp = batchGetInstances(cluster, instances);
		if (!credisResp.isSuccess()) {
			throw new IllegalStateException("[batchQueryInstanceStatus][" + cluster + "]" + credisResp.getMessage());
		}

		List<InstanceStatus> credisInstances = credisResp.getResult();
		Map<HostPort, Boolean> result = new HashMap<>();
		for (InstanceStatus credisInstance: credisInstances) {
			HostPort instance = new HostPort(credisInstance.getIPAddress(), credisInstance.getPort());
			if (instances.contains(instance)) {
				result.put(instance, credisInstance.isCanRead());
			}
		}

		return result;
	}

	@Override
	public void markInstanceDown(ClusterShardHostPort clusterShardHostPort) throws OuterClientException {
		doMarkInstance(clusterShardHostPort, false);
	}

	@Override
	public void markInstanceDownIfNoModifyFor(ClusterShardHostPort clusterShardHostPort, long noModifySeconds) throws OuterClientException {
		doMarkInstanceIfNoModifyFor(clusterShardHostPort, false, noModifySeconds);
	}

	private void doMarkInstanceIfNoModifyFor(ClusterShardHostPort clusterShardHostPort, boolean state, long noModifySeconds) throws OuterClientException {

		try {
			catTransactionMonitor.logTransaction(TYPE, String.format("doMarkInstanceIfNoModifyFor-%s", clusterShardHostPort.getClusterName()), new Task() {
				@Override
				public void go() throws Exception {

					logger.info("[doMarkInstanceIfNoModifyFor][begin]{},{},{}", clusterShardHostPort, state, noModifySeconds);
					String address = CREDIS_SERVICE.SWITCH_STATUS.getRealPath(credisConfig.getCredisServiceAddress());
					String cluster = clusterShardHostPort.getClusterName();
					HostPort hostPort = clusterShardHostPort.getHostPort();
					String reqType = state ? "markInstanceUpIfNoModify" : "markInstanceDownIfNoModify";

					MarkInstanceResponse response = doRequest(reqType, cluster, () ->
							restOperations.postForObject(
									address + "?clusterName={cluster}&ip={ip}&port={port}&canRead={canRead}&noModifySeconds={noModifySeconds}",
									null, MarkInstanceResponse.class, cluster, hostPort.getHost(), hostPort.getPort(), state, noModifySeconds)
					);
					logger.info("[doMarkInstanceIfNoModifyFor][end]{},{},{},{}", clusterShardHostPort, state, noModifySeconds, response);
					if(!response.isSuccess()){
						throw new IllegalStateException(String.format("%s %s, response:%s", clusterShardHostPort, state, response));
					}
				}

				@Override
				public Map getData() {
					return new HashMap<String, Object>() {{
						put("cluster", clusterShardHostPort.getClusterName());
						put("shard", clusterShardHostPort.getShardName());
						put("hostport", clusterShardHostPort.getHostPort());
						put("state", state);
						put("noModifySeconds", noModifySeconds);
					}};
				}
			});
		} catch (Exception e) {
			throw new OuterClientException("mark:" + clusterShardHostPort+ ":" + state, e);
		}

	}

	private void doMarkInstance(ClusterShardHostPort clusterShardHostPort, boolean state) throws OuterClientException {

		try {
			catTransactionMonitor.logTransaction(TYPE, String.format("doMarkInstance-%s", clusterShardHostPort.getClusterName()), new Task() {
                @Override
                public void go() throws Exception {

					logger.info("[doMarkInstance][begin]{},{}", clusterShardHostPort, state);
					String address = CREDIS_SERVICE.SWITCH_STATUS.getRealPath(credisConfig.getCredisServiceAddress());
					String cluster = clusterShardHostPort.getClusterName();
                    HostPort hostPort = clusterShardHostPort.getHostPort();
                    String reqType = state ? "markInstanceUp" : "markInstanceDown";

                    MarkInstanceResponse response = doRequest(reqType, cluster, () -> {
								UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(address);
								uriBuilder.queryParam("clusterName", cluster)
										.queryParam("ip", hostPort.getHost())
										.queryParam("port", hostPort.getPort())
										.queryParam("canRead", state);
								if (null != clusterShardHostPort.getActiveDc()) {
									uriBuilder.queryParam("activeDc", clusterShardHostPort.getActiveDc());
								}
								return restOperations.postForObject(uriBuilder.toUriString(), null, MarkInstanceResponse.class);
							}
					);
                    logger.info("[doMarkInstance][end]{},{},{}", clusterShardHostPort, state, response);
                    if(!response.isSuccess()){
                        throw new IllegalStateException(String.format("%s %s, response:%s", clusterShardHostPort, state, response));
                    }
				}

				@Override
				public Map getData() {
					return new HashMap<String, Object>() {{
						put("cluster", clusterShardHostPort.getClusterName());
						put("shard", clusterShardHostPort.getShardName());
						put("hostport", clusterShardHostPort.getHostPort());
						put("state", state);
					}};
				}
            });
		} catch (Exception e) {
			throw new OuterClientException("mark:" + clusterShardHostPort+ ":" + state, e);
		}

	}

	@Override
	public void batchMarkInstance(OuterClientService.MarkInstanceRequest markInstanceRequest) throws OuterClientException {
		try {
			catTransactionMonitor.logTransaction(TYPE, String.format("doBatchMarkInstance-%s", markInstanceRequest.getClusterName()), new Task() {
				@Override
				public void go() throws Exception {

					logger.info("[doBatchMarkInstance][begin]{}", markInstanceRequest);
					String address = CREDIS_SERVICE.BATCH_SWITCH_STATUS.getRealPath(credisConfig.getCredisServiceAddress());
					String reqType = "batchMarkInstance";

					MarkInstanceResponse response = doRequest(reqType, markInstanceRequest.getClusterName(),
							() -> restOperations.postForObject(address, markInstanceRequest, MarkInstanceResponse.class));
					logger.info("[doBatchMarkInstance][end]{},{}", markInstanceRequest, response);
					if(!response.isSuccess()){
						throw new IllegalStateException(String.format("%s, response:%s", markInstanceRequest, response));
					}
				}

				@Override
				public Map getData() {
					return new HashMap<String, Object>() {{
						put("cluster", markInstanceRequest.getClusterName());
						put("hostPortDcStatuses", markInstanceRequest.getHostPortDcStatuses());
						put("activeDc", markInstanceRequest.getActiveDc());
					}};
				}
			});
		} catch (Exception e) {
			throw new OuterClientException("batch mark:" + markInstanceRequest, e);
		}

	}

	@Override
	public boolean clusterMigratePreCheck(String clusterName) throws OuterClientException {
		try {

			return catTransactionMonitor.logTransaction(TYPE, String.format("clusterMigratePreCheck-%s", clusterName), new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					logger.info("[clusterMigratePreCheck]Cluster:{}", clusterName);
					String credisAddress = CREDIS_SERVICE.MIGRATION_PRE_CHECK.getRealPath(credisConfig.getCredisServiceAddress());
					return doRequest("clusterMigratePreCheck", clusterName, () ->
							restOperations.postForObject(credisAddress, null, Boolean.class, clusterName));
				}
			});

		} catch (Exception e) {
			throw new OuterClientException(String.format("%s pre check fail", clusterName), e);
		}
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) throws OuterClientException {

		try {

			return catTransactionMonitor.logTransaction(TYPE, String.format("doMigrationPublish-%s-%s", clusterName, primaryDcName), new Callable<MigrationPublishResult>() {

                @Override
                public MigrationPublishResult call() throws Exception {

                    logger.info("[doMigrationPublish]Cluster:{}, NewPrimaryDc:{} -> ConvertedDcName:{} , NewMasters:{}", clusterName, primaryDcName,convertDcName(primaryDcName), newMasters);
                    String credisAddress = CREDIS_SERVICE.MIGRATION_PUBLISH.getRealPath(credisConfig.getCredisServiceAddress());
                    String startTime = DateTimeUtils.currentTimeAsString();
                    MigrationPublishResult res = doRequest("doMigrationPublish", clusterName,
							() -> restOperations.postForObject(credisAddress, newMasters, MigrationPublishResult.class,
									clusterName, convertDcName(primaryDcName)));


                    String endTime = DateTimeUtils.currentTimeAsString();
                    res.setPublishAddress(credisAddress);
                    res.setClusterName(clusterName);
                    res.setPrimaryDcName(primaryDcName);
                    res.setNewMasters(newMasters);
                    res.setStartTime(startTime);
                    res.setEndTime(endTime);
                    return res;
                }
            });
		} catch (Exception e) {
			throw new OuterClientException(String.format("%s:%s,%s", clusterName, primaryDcName, newMasters), e);
		}
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
			InetSocketAddress newMaster) throws OuterClientException {

		return doMigrationPublish(clusterName, primaryDcName, Arrays.asList(newMaster));
	}

	@Override
	public ClusterInfo getClusterInfo(String clusterName) throws Exception {


		return catTransactionMonitor.logTransaction(TYPE, String.format("getClusterInfo:%s", clusterName), new Callable<ClusterInfo>() {
			@Override
			public ClusterInfo call() throws Exception {

				String address = CREDIS_SERVICE.QUERY_CLUSTER.getRealPath(credisConfig.getCredisServiceAddress());
				ClusterInfo clusterInfo = doRequest("getClusterInfo", clusterName,
						() -> restOperations.getForObject(address + "?name={clusterName}", ClusterInfo.class, clusterName));
				clusterInfo.mapIdc(DC_TRANSFORM_DIRECTION.OUTER_TO_INNER);
				return clusterInfo;
			}
		});
	}

	@Override
	public List<ClusterInfo> getActiveDcClusters(String dc) throws Exception {
		return catTransactionMonitor.logTransaction(TYPE, String.format("getActiveDcClusters:%s", dc), new Callable<List<ClusterInfo>>() {
			@Override
			public List<ClusterInfo> call() throws Exception {
				String address = CREDIS_SERVICE.QUERY_CLUSTERS.getRealPath(credisConfig.getCredisServiceAddress());
				List<ClusterInfo> clusters = doRequest("getActiveDcClusters", null,
						() -> restOperations
								.exchange(address + "?activeDc={clusterName}", HttpMethod.GET, null, clustersRespTypeDef, convertDcName(dc))
								.getBody());
				clusters.forEach(cluster -> cluster.mapIdc(DC_TRANSFORM_DIRECTION.OUTER_TO_INNER));
				return clusters;
			}
		});
	}

	@Override
	public DcMeta getOutClientDcMeta(String dc) throws Exception {
		return catTransactionMonitor.logTransaction(TYPE, String.format("getIdcClusters:%s", dc), new Callable<DcMeta>() {
			@Override
			public DcMeta call() throws Exception {

				String address = CREDIS_SERVICE.QUERY_DC_META.getRealPath(credisConfig.getCredisServiceAddress());
				DcMeta dcMeta = doRequest("getIdcClusters", null,
						() -> restOperations.getForObject(address + "?idc={dc}", DcMeta.class, dc));
				dcMeta.mapIdc(DC_TRANSFORM_DIRECTION.OUTER_TO_INNER);
				return dcMeta;
			}
		});
	}

	@Override
	public boolean excludeIdcs(String clusterName, String[] idcs) throws Exception {
		String NAME = "exclude idcs: " + String.join(",", idcs);
		return catTransactionMonitor.logTransaction(TYPE, NAME, new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
			    String address = CREDIS_SERVICE.EXCLUDE_IDCS.getRealPath(credisConfig.getCredisServiceAddress());
				SimpleResult result = doRequest("excludeClusterDc", clusterName,
						() -> restOperations.postForObject(address, idcs, SimpleResult.class, clusterName));
				if (result != null && result.getSuccess()) {
					CatEventMonitor.DEFAULT.logEvent(TYPE, NAME + " - success");
					return true;
				}
				CatEventMonitor.DEFAULT.logEvent(TYPE, NAME + " - " + result.getMessage());
				return false;
			}
		});
	}

	@Override
	public boolean batchExcludeIdcs(List<ClusterExcludedIdcInfo> excludedClusterIdcs) throws Exception {
		String NAME = "batch-exclude-idcs";
		return catTransactionMonitor.logTransaction(TYPE, NAME, new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				logger.info("[batchExcludeIdcs] {}", Codec.DEFAULT.encode(excludedClusterIdcs));
				String address = CREDIS_SERVICE.BATCH_EXCLUDE_IDCS.getRealPath(credisConfig.getCredisServiceAddress());
				SimpleResult result = doRequest("batchExcludeClusterDc", null,
						() -> restOperations.postForObject(address, excludedClusterIdcs, SimpleResult.class));
				if (result != null && result.getSuccess()) {
					CatEventMonitor.DEFAULT.logEvent(TYPE, NAME + " - success");
					logger.info("[batchExcludeIdcs][success]");
					return true;
				}
				String failMsg = null == result ? "null" : result.getMessage();
				CatEventMonitor.DEFAULT.logEvent(TYPE, NAME + " - " + failMsg);
				logger.info("[batchExcludeIdcs][fail] {}", failMsg);
				return false;
			}
		});
	}

	@Override
	public OuterClientDataResp<List<ClusterExcludedIdcInfo>> getAllExcludedIdcs() throws Exception {
		return catTransactionMonitor.logTransaction(TYPE, "getAllExcludedIdcs", () -> {
			String address = CREDIS_SERVICE.BATCH_EXCLUDE_IDCS.getRealPath(credisConfig.getCredisServiceAddress());
			return doRequest("getAllExcludedIdcs", null,
					() -> restOperations
							.exchange(address, HttpMethod.GET, null, excludedIdcInfosRespTypeDef)
							.getBody());
		});
	}

	String convertDcName(String dc) {
		return DcMapper.INSTANCE.getDc(dc);
	}

	public GetInstanceResult getInstance(ClusterShardHostPort clusterShardHostPort) throws OuterClientException {

		try {
			return catTransactionMonitor.logTransaction(TYPE, "getInstance", new Callable<GetInstanceResult>() {
                @Override
                public GetInstanceResult call() throws Exception {

                	HostPort hostPort = clusterShardHostPort.getHostPort();
                	String cluster = clusterShardHostPort.getClusterName();
                    String address = CREDIS_SERVICE.QUERY_STATUS.getRealPath(credisConfig.getCredisServiceAddress());
                    GetInstanceResult result = doRequest("getInstance", cluster,
							() -> restOperations.getForObject(address + "?ip={ip}&port={port}",
									GetInstanceResult.class, hostPort.getHost(), hostPort.getPort()));
                    return result;
                }
            });
		} catch (Exception e) {
			throw new OuterClientException("getInstance:" + clusterShardHostPort, e);
		}
	}

	private static final ParameterizedTypeReference<CRedisGeneralResp<List<InstanceStatus>>> batchInstancesRespType =
			new ParameterizedTypeReference<CRedisGeneralResp<List<InstanceStatus>>>(){};
	public CRedisGeneralResp<List<InstanceStatus>> batchGetInstances(String cluster, Set<HostPort> instances) throws OuterClientException {

		try {
			return catTransactionMonitor.logTransaction(TYPE, "batchGetInstances", new Callable<CRedisGeneralResp<List<InstanceStatus>>>() {
				@Override
				public CRedisGeneralResp<List<InstanceStatus>> call() throws Exception {

					String address = CREDIS_SERVICE.BATCH_QUERY_STATUS.getRealPath(credisConfig.getCredisServiceAddress());
					ResponseEntity<CRedisGeneralResp<List<InstanceStatus>>> resp = doRequest("batchGetInstances", cluster,
							() -> {
								HttpEntity<Set<HostPort>> httpEntity = new HttpEntity<>(instances);
						return restOperations.exchange(address, HttpMethod.POST, httpEntity, batchInstancesRespType);
							});
					return resp.getBody();
				}
			});
		} catch (Exception e) {
			throw new OuterClientException("batchGetInstances:" + cluster, e);
		}
	}

	public <T> T doRequest(String apiType, String cluster, Callable<T> caller) throws Exception {
		long startTime = System.currentTimeMillis();
		T response = null;

		try {
			response = caller.call();
			return response;
		} finally {
			long endTime = System.currentTimeMillis();
			boolean success;
			if (response instanceof CRedisResp) success = ((CRedisResp) response).isSuccess();
			else success = null != response;

			tryMetric(apiType, success, cluster, startTime, endTime);
		}
	}

	public void tryMetric(String apiType, boolean status, String clusterName, long startTime, long endTime) {
		MetricData metricData = new MetricData("call.credis", null,
				StringUtil.isEmpty(clusterName) ? "-" : clusterName, null);
		metricData.setValue((double)(endTime - startTime));
		metricData.setTimestampMilli(startTime);
		metricData.addTag("api", apiType);
		metricData.addTag("status", status ? "SUCCESS":"FAIL");

		try {
			metricProxy.writeBinMultiDataPoint(metricData);
		} catch (Throwable th) {
			logger.debug("[tryMetric] fail", th);
		}
	}

	public interface CRedisResp {
		boolean isSuccess();
	}

	@VisibleForTesting
	protected void setMetricProxy(MetricProxy metricProxy) {
		this.metricProxy = metricProxy;
	}

	public static class SimpleResult extends AbstractInfo implements CRedisResp {

		private boolean success;
		private String message;

		public SimpleResult() {

		}

		public SimpleResult(boolean success, String message) {
			this.success = success;
			this.message = message;
		}

		public boolean getSuccess() {
			return success;
		}

		public void setSuccess(boolean success) {
			this.success = success;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		@Override
		public boolean isSuccess() {
			return success;
		}
	}

	public static class MarkInstanceResponse implements CRedisResp {

		private boolean success;
		private String message;

		public boolean isSuccess() {
			return success;
		}

		public void setSuccess(boolean success) {
			this.success = success;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		@Override
		public String toString() {
			return String.format("success:%s, message:%s", success, message);
		}
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class CRedisGeneralResp<T> implements CRedisResp {

		private boolean success;

		private String message;

		private T result;

		public CRedisGeneralResp() {

		}

		@Override
		public boolean isSuccess() {
			return success;
		}

		public String getMessage() {
			return message;
		}

		public T getResult() {
			return result;
		}
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class GetInstanceResult implements CRedisResp {

		private boolean success;

		private String message;

		private InstanceStatus result;

		public InstanceStatus getResult() {
			return result;
		}

		public void setResult(InstanceStatus result) {
			this.result = result;
		}
		public boolean isSuccess() {
			return success;
		}

		public void setSuccess(boolean success) {
			this.success = success;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		@Override
		public String toString() {
			return String.format("success:%s, message:%s, result:%s", success, message, result);
		}
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class InstanceStatus{

		private boolean canRead;
		private String  env;
		private String IPAddress;
		private int port;

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public String getIPAddress() {

			return IPAddress;
		}

		public void setIPAddress(String IPAddress) {
			this.IPAddress = IPAddress;
		}

		public String getEnv() {
			return env;
		}

		public void setEnv(String env) {
			this.env = env;
		}

		public boolean isCanRead() {
			return canRead;
		}

		public void setCanRead(boolean canRead) {
			this.canRead = canRead;
		}

		@Override
		public String toString() {
			return String.format("canRead:%s, env:%s, %s:%d", canRead, env, IPAddress, port);
		}
	}


}
