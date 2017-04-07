package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.migration.MigrationPublishService;
import com.ctrip.xpipe.api.migration.MigrationPublishService.MigrationPublishResult;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
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

	protected MigrationPublishService publishService = MigrationPublishService.DEFAULT;
	
	public AbstractMigrationPublishState(MigrationCluster holder, MigrationStatus status) {
		super(holder, status);
	}
	
	public MigrationPublishService getMigrationPublishService() {
		return publishService;
	}

	
	protected boolean publish() {
		String cluster = getHolder().getCurrentCluster().getClusterName();
		String newPrimaryDc = getHolder().getClusterDcs().get(getHolder().getMigrationCluster().getDestinationDcId()).getDcName();
		List<InetSocketAddress> newMasters = new LinkedList<>();
		for(ShardTbl shard : getHolder().getClusterShards().values()) {
			InetSocketAddress addr = null;
			try {
				addr = getMasterAddress(getHolder().getRedisService().findAllByDcClusterShard(newPrimaryDc, cluster, shard.getShardName()));
				if(null != addr) {
					newMasters.add(addr);
				}
			} catch (ResourceNotFoundException e) {
				throw new IllegalStateException("[publish]", e);
			}
		}
		
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
	
	private InetSocketAddress getMasterAddress(List<RedisTbl> redises) {
		InetSocketAddress res = null;
		for(RedisTbl redis : redises) {
			if(redis.isMaster()) {
				res = InetSocketAddress.createUnresolved(redis.getRedisIp(), redis.getRedisPort());
			}
		}
		return res;
	}
	
	private void updateMigrationPublishResult(MigrationPublishResult res) {
		if(null != res) {
			MigrationClusterTbl migrationCluster = getHolder().getMigrationCluster();
			migrationCluster.setPublishInfo(res.toString());
			getHolder().getMigrationService().updateMigrationCluster(migrationCluster);
		}
	}
	
	@Override
	public void refresh() {
		// Nothing to do
		logger.debug("[]{}",getClass().toString(), getHolder().getCurrentCluster().getClusterName());
	}
}
