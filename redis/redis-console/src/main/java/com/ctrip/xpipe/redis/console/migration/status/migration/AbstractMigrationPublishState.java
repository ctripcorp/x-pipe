package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.migration.OuterClientService.MigrationPublishResult;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;

/**
 * @author shyin
 *
 * Dec 30, 2016
 */
public abstract class AbstractMigrationPublishState extends AbstractMigrationState {

	protected OuterClientService publishService = OuterClientService.DEFAULT;
	
	public AbstractMigrationPublishState(MigrationCluster holder, MigrationStatus status) {
		super(holder, status);
	}
	
	public OuterClientService getMigrationPublishService() {
		return publishService;
	}

	
	protected boolean publish() {

		String cluster = getHolder().clusterName();
		String newPrimaryDc = getHolder().destDc();

		List<InetSocketAddress> newMasters = getNewMasters();

		boolean ret = false;
		MigrationPublishResult res = null;
		try {
			res = getMigrationPublishService().doMigrationPublish(cluster, newPrimaryDc, newMasters);
			logger.info("[MigrationPublishStat][result]{}",res);
			ret = res.isSuccess();
		} catch (Exception e) {
			res = new MigrationPublishResult("", cluster, newPrimaryDc, newMasters);
			res.setSuccess(false);
			res.setMessage(e.getMessage());
			logger.error("[MigrationPublish][fail]",e);
			ret = false;
		}
		
		updateMigrationPublishResult(res);
		
		return ret;
 	}
	
	private void updateMigrationPublishResult(MigrationPublishResult res) {
		if(null != res) {
			getHolder().updatePublishInfo(res.toString());
		}
	}
	
	@Override
	public void refresh() {
		// Nothing to do
		logger.debug("[]{}",getClass().toString(), getHolder().clusterName());
	}

	public List<InetSocketAddress> getNewMasters() {

		List<InetSocketAddress> result = new LinkedList<>();
		for(MigrationShard migrationShard : getHolder().getMigrationShards()){
			HostPort newMasterAddress = migrationShard.getNewMasterAddress();
			if(newMasterAddress == null){
				//may force publish
				logger.warn("[getNewMasters][null master]{}", migrationShard.shardName());
				continue;
			}
			result.add(InetSocketAddress.createUnresolved(newMasterAddress.getHost(), newMasterAddress.getPort()));
		}
		return result;
	}
}
