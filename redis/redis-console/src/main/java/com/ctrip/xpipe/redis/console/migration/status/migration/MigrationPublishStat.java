package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.migration.MigrationPublishService;
import com.ctrip.xpipe.api.migration.MigrationPublishService.MigrationPublishResult;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationPublishStat extends AbstractMigrationStat {
	
	private MigrationPublishService publishService = MigrationPublishService.DEFAULT;
	
	public MigrationPublishStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Publish);
		this.setNextAfterSuccess(new MigrationSuccessStat(getHolder()))
			.setNextAfterFail(this);
	}
	
	public MigrationPublishService getMigrationPublishService() {
		return publishService;
	}

	@Override
	public void action() {
		updateDB();
		
		if(publish()) {
			updateAndProcess(nextAfterSuccess(), true);
		} else {
			updateAndProcess(nextAfterFail(), false);
		}
	}

	@DalTransaction
	private void updateDB() {
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setActivedcId(getHolder().getMigrationCluster().getDestinationDcId());
		cluster.setStatus(ClusterStatus.TmpMigrated.toString());
		getHolder().getClusterService().update(cluster);

		MigrationClusterTbl migrationClusterTbl = getHolder().getMigrationCluster();
		migrationClusterTbl.setEndTime(new Date());
		migrationClusterTbl.setStatus(MigrationStatus.Publish.toString());
		getHolder().getMigrationService().updateMigrationCluster(migrationClusterTbl);
		
	}
	
	private boolean publish() {
		String cluster = getHolder().getCurrentCluster().getClusterName();
		String newPrimaryDc = getHolder().getClusterDcs().get(getHolder().getMigrationCluster().getDestinationDcId()).getDcName();
		List<InetSocketAddress> newMasters = new LinkedList<>();
		for(ShardTbl shard : getHolder().getClusterShards().values()) {
			InetSocketAddress addr = getMasterAddress(getHolder().getRedisService().findAllByDcClusterShard(newPrimaryDc, cluster, shard.getShardName()));
			if(null != addr) {
				newMasters.add(addr);
			}
		}
		
		boolean ret = false;
		try {
			MigrationPublishResult res = getMigrationPublishService().doMigrationPublish(cluster, newPrimaryDc, newMasters);
			logger.info("[MigrationPublishStat][result]{}",res);
			ret = res.isSuccess();
		} catch (Exception e) {
			logger.error("[MigrationPublish][fail]",e);
			ret = false;
		}
		
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

	@Override
	public void refresh() {
		// Nothing to do
		logger.info("[MigrationPublish]{}", getHolder().getCurrentCluster().getClusterName());
	}
}
