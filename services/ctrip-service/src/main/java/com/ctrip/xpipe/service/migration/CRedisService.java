package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.api.migration.DC_TRANSFORM_DIRECTION;
import com.ctrip.xpipe.api.migration.DcMapper;
import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.migration.AbstractOuterClientService;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.monitor.CatTransactionMonitor;
import com.ctrip.xpipe.service.beacon.data.BeaconResp;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

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
	public boolean isInstanceUp(ClusterShardHostPort clusterShardHostPort) throws OuterClientException {

		GetInstanceResult instance = getInstance(clusterShardHostPort);

		if(instance.isSuccess()){
			return instance.getResult().isCanRead();
		}
		throw new IllegalStateException("[isInstanceUp]" + clusterShardHostPort + "," + instance.getMessage());
	}

	@Override
	public void markInstanceDown(ClusterShardHostPort clusterShardHostPort) throws OuterClientException {
		doMarkInstance(clusterShardHostPort, false);
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

                    MarkInstanceResponse response = doRequest(reqType, cluster, () ->
							restOperations.postForObject(address + "?clusterName={cluster}&ip={ip}&port={port}&canRead={canRead}",
							null, MarkInstanceResponse.class, cluster, hostPort.getHost(), hostPort.getPort(), state)
					);
                    logger.info("[doMarkInstance][ end ]{},{},{}", clusterShardHostPort, state, response);
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

	String convertDcName(String dc) {
		return DcMapper.INSTANCE.getDc(dc);
	}

	public GetInstanceResult getInstance(ClusterShardHostPort clusterShardHostPort) throws OuterClientException {

		try {
			return catTransactionMonitor.logTransaction(TYPE, String.format("getInstance"), new Callable<GetInstanceResult>() {
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

	public static class MarkInstanceRequest{

		private String ip;
		private int port;
		private boolean canRead;

		public MarkInstanceRequest(String ip, int port, boolean canRead){
			this.ip = ip;
			this.port = port;
			this.canRead = canRead;
		}

		public String getIp() {
			return ip;
		}

		public int getPort() {
			return port;
		}

		public boolean isCanRead() {
			return canRead;
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
