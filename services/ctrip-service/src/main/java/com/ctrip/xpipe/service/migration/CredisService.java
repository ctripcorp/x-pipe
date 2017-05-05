package com.ctrip.xpipe.service.migration;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import com.ctrip.xpipe.api.migration.DcMapper;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.migration.AbstractOuterClientService;
import com.ctrip.xpipe.monitor.CatTransactionMonitor;
import com.ctrip.xpipe.utils.DateTimeUtils;
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
	public void markInstanceUp(HostPort hostPort) throws Exception {
		doMarkInstance(hostPort, true);
	}

	@Override
	public void markInstanceDown(HostPort hostPort) throws Exception {
		doMarkInstance(hostPort, false);

	}

	private void doMarkInstance(HostPort hostPort, boolean state) throws Exception {

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

	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) throws Exception {

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
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
			InetSocketAddress newMaster) throws Exception {

		return doMigrationPublish(clusterName, primaryDcName, Arrays.asList(newMaster));
	}
	
	String convertDcName(String dc) {
		return DcMapper.INSTANCE.getDc(dc);
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

}
