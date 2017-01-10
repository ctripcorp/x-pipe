package com.ctrip.xpipe.migration;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public class DefaultMigrationPublishService extends AbstractMigrationPublishService {

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) {
		logger.info("[doMigrationPublish]Cluster:{}, NewPrimaryDc:{}, Masters:{}", clusterName, primaryDcName,
				newMasters);
		MigrationPublishResult res = new MigrationPublishResult("default-addr", clusterName, primaryDcName, newMasters);
		res.setSuccess(true);res.setMessage("default-success");
		return res;
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
			InetSocketAddress newMaster) {
		logger.info("[doMigrationPublish]Cluster:{}, Shard:{}, NewPrimaryDc:{}, NewMaster:{}", clusterName, shardName,
				primaryDcName, newMaster);
		MigrationPublishResult res = new MigrationPublishResult("default-addr", clusterName, primaryDcName, Arrays.asList(newMaster));
		res.setSuccess(true);res.setMessage("default-success");
		return res;
	}

}
