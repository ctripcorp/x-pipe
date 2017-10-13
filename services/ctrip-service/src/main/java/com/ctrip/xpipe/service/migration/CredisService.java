package com.ctrip.xpipe.service.migration;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import com.ctrip.xpipe.api.migration.DcMapper;
import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.migration.AbstractOuterClientService;
import com.ctrip.xpipe.monitor.CatTransactionMonitor;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import org.springframework.web.client.RestOperations;

import com.ctrip.xpipe.spring.RestTemplateFactory;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public class CredisService extends AbstractOuterClientService {

	RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(10, 100);

	private CatTransactionMonitor catTransactionMonitor = new CatTransactionMonitor();

	private CredisConfig credisConfig = CredisConfig.INSTANCE;

	private final String TYPE = "credis";

	@Override
	public int getOrder() {
		return HIGHEST_PRECEDENCE;
	}

	@Override
	public void markInstanceUp(HostPort hostPort) throws OuterClientException {
		doMarkInstance(hostPort, true);
	}

	@Override
	public boolean isInstanceUp(HostPort hostPort) throws OuterClientException {

		GetInstanceResult instance = getInstance(hostPort);

		if(instance.isSuccess()){
			return instance.getResult().isCanRead();
		}
		throw new IllegalStateException("[isInstanceUp]" + hostPort + "," + instance.getMessage());
	}

	@Override
	public void markInstanceDown(HostPort hostPort) throws OuterClientException {
		doMarkInstance(hostPort, false);

	}

	private void doMarkInstance(HostPort hostPort, boolean state) throws OuterClientException {

		try {

			catTransactionMonitor.logTransaction(TYPE, String.format("doMarkInstance-%s-%s", hostPort, state), new Task() {
                @Override
                public void go() throws Exception {

                    String address = CREDIS_SERVICE.SWITCH_STATUS.getRealPath(credisConfig.getCredisServiceAddress());
                    MarkInstanceResponse response =
                            restOperations.postForObject(address, new MarkInstanceRequest(hostPort.getHost(), hostPort.getPort(), state), MarkInstanceResponse.class);
                    logger.info("[doMarkInstance]{},{},{}", hostPort, state, response);
                    if(!response.isSuccess()){
                        throw new IllegalStateException(String.format("%s %s, response:%s", hostPort, state, response));
                    }
                }
            });
		} catch (Exception e) {
			throw new OuterClientException("mark:" + hostPort+ ":" + state, e);
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
                    MigrationPublishResult res = restOperations.postForObject(credisAddress,
                            newMasters, MigrationPublishResult.class, clusterName, convertDcName(primaryDcName));
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
	
	String convertDcName(String dc) {
		return DcMapper.INSTANCE.getDc(dc);
	}

	public GetInstanceResult getInstance(HostPort hostPort) throws OuterClientException {

		try {
			return catTransactionMonitor.logTransaction(TYPE, String.format("getInstance"), new Callable<GetInstanceResult>() {
                @Override
                public GetInstanceResult call() throws Exception {
                    String address = CREDIS_SERVICE.Query_STATUS.getRealPath(credisConfig.getCredisServiceAddress());
                    GetInstanceResult result = restOperations.getForObject(address + "?ip={ip}&port={port}", GetInstanceResult.class, hostPort.getHost(), hostPort.getPort());
                    return result;
                }
            });
		} catch (Exception e) {
			throw new OuterClientException("getInstance:" + hostPort, e);
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

	public static class MarkInstanceResponse{

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
	public static class GetInstanceResult{

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
