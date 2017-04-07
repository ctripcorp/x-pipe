package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationPublishState extends AbstractMigrationPublishState {
	
	public MigrationPublishState(MigrationCluster holder) {
		super(holder, MigrationStatus.Publish);
		this.setNextAfterSuccess(new MigrationSuccessState(getHolder()))
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		try {
			updateRedisMaster();
		} catch (ResourceNotFoundException e) {
			logger.error("[action]", e);
		}

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
		
	}
	

	private void updateRedisMaster() throws ResourceNotFoundException {
		List<RedisTbl> toUpdate = new LinkedList<>();
		
		MigrationCluster migrationCluster = getHolder();
		ClusterTbl cluster = migrationCluster.getCurrentCluster();
		for(MigrationShard shard : migrationCluster.getMigrationShards()) {
			List<RedisTbl> prevDcRedises = migrationCluster.getRedisService().findAllByDcClusterShard(
					migrationCluster.getClusterDcs().get(cluster.getActivedcId()).getDcName(),
					cluster.getClusterName(),
					shard.getCurrentShard().getShardName());
			for(RedisTbl redis : prevDcRedises) {
				if(redis.isMaster()) {
					redis.setMaster(false);
					toUpdate.add(redis);
				}
			}
			
			if(null != shard.getNewMasterAddress()) {
				List<RedisTbl> newDcRedises = migrationCluster.getRedisService().findAllByDcClusterShard(
						migrationCluster.getClusterDcs().get(migrationCluster.getMigrationCluster().getDestinationDcId()).getDcName(),
						cluster.getClusterName(),
						shard.getCurrentShard().getShardName());
				for(RedisTbl redis : newDcRedises) {
					if(redis.getRedisIp().equals(shard.getNewMasterAddress().getHostName()) && redis.getRedisPort() == shard.getNewMasterAddress().getPort()) {
						redis.setMaster(true);
						toUpdate.add(redis);
					}
				}
			}
		}
		
		logger.info("[UpdateMaster]");
		migrationCluster.getRedisService().batchUpdate(toUpdate);
		
	}
	
}
