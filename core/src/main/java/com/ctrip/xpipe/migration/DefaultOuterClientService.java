package com.ctrip.xpipe.migration;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.utils.DateTimeUtils;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public class DefaultOuterClientService extends AbstractOuterClientService {

	private Map<HostPort, Boolean> instanceStatus = new ConcurrentHashMap<>();

	@Override
	public void markInstanceUp(HostPort hostPort) throws OuterClientException {
		logger.info("[markInstanceUp]{}", hostPort);
		instanceStatus.put(hostPort, true);

	}

	@Override
	public boolean isInstanceUp(HostPort hostPort) throws OuterClientException {

		Boolean result = instanceStatus.get(hostPort);
		if(result == null){
			return Boolean.parseBoolean(System.getProperty("InstanceUp", "true"));
		}
		return result;
	}

	@Override
	public void markInstanceDown(HostPort hostPort) throws OuterClientException {
		logger.info("[markInstanceDown]{}", hostPort);
		instanceStatus.put(hostPort, false);
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) throws OuterClientException{
		logger.info("[doMigrationPublish]Cluster:{}, NewPrimaryDc:{}, Masters:{}", clusterName, primaryDcName,
				newMasters);
		String startTime = DateTimeUtils.currentTimeAsString();
		MigrationPublishResult res = new MigrationPublishResult("default-addr", clusterName, primaryDcName, newMasters);
		String endTime = DateTimeUtils.currentTimeAsString();
		res.setSuccess(true);res.setMessage("default-success");
		res.setStartTime(startTime);
		res.setEndTime(endTime);
		return res;
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
			InetSocketAddress newMaster) throws OuterClientException{

		logger.info("[doMigrationPublish]Cluster:{}, Shard:{}, NewPrimaryDc:{}, NewMaster:{}", clusterName, shardName,
				primaryDcName, newMaster);
		String startTime = DateTimeUtils.currentTimeAsString();
		MigrationPublishResult res = new MigrationPublishResult("default-addr", clusterName, primaryDcName, Arrays.asList(newMaster));
		String endTime = DateTimeUtils.currentTimeAsString();
		res.setSuccess(true);res.setMessage("default-success");
		res.setStartTime(startTime);
		res.setEndTime(endTime);
		return res;
	}

}
