package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.migration.MigrationPublishService;
import com.ctrip.xpipe.api.migration.MigrationPublishService.MigrationPublishResult;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;

/**
 * @author shyin
 *
 * Dec 30, 2016
 */
public abstract class AbstractMigrationPublishStat extends AbstractMigrationStat {

	protected MigrationPublishService publishService = MigrationPublishService.DEFAULT;
	
	public AbstractMigrationPublishStat(MigrationCluster holder, MigrationStatus status) {
		super(holder, status);
	}
	
	public MigrationPublishService getMigrationPublishService() {
		return publishService;
	}

	
	protected boolean publish() {
		String cluster = getHolder().getCurrentCluster().getClusterName();
		String newPirmaryDc = getHolder().getClusterDcs().get(getHolder().getMigrationCluster().getDestinationDcId()).getDcName();
		List<InetSocketAddress> newMasters = new LinkedList<>();
		for(ShardTbl shard : getHolder().getClusterShards().values()) {
			InetSocketAddress addr = getMasterAddress(getHolder().getRedisService().findAllByDcClusterShard(newPirmaryDc, cluster, shard.getShardName()));
			if(null != addr) {
				newMasters.add(addr);
			}
		}
		
		boolean ret = false;
		try {
			MigrationPublishResult res = getMigrationPublishService().doMigrationPublish(cluster, newPirmaryDc, newMasters);
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
		logger.debug("[]{}",getClass().toString(), getHolder().getCurrentCluster().getClusterName());
	}
}
