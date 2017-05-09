package com.ctrip.xpipe.migration;

import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.utils.DateTimeUtils;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public class DefaultOuterClientService extends AbstractOuterClientService {

	@Override
	public void markInstanceUp(HostPort hostPort) throws Exception {
		logger.info("[markInstanceUp]{}", hostPort);
	}

	@Override
	public boolean isInstanceUp(HostPort hostPort) throws Exception {
		return Boolean.parseBoolean(System.getProperty("InstanceUp", "true"));
	}

	@Override
	public void markInstanceDown(HostPort hostPort) throws Exception {
		logger.info("[markInstanceDown]{}", hostPort);

	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) {
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
			InetSocketAddress newMaster) {

		logger.info("[doMigrationPublish]Cluster:{}, Shard:{}, NewPrimaryDc:{}, NewMaster:{}", clusterName, shardName,
				primaryDcName, newMaster);
		String startTime = DateTimeUtils.currentTimeAsString();;
		MigrationPublishResult res = new MigrationPublishResult("default-addr", clusterName, primaryDcName, Arrays.asList(newMaster));
		String endTime = DateTimeUtils.currentTimeAsString();;
		res.setSuccess(true);res.setMessage("default-success");
		res.setStartTime(startTime);
		res.setEndTime(endTime);
		return res;
	}

}
