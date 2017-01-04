package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;

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
		updateRedisMaster();
		
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
		
		logger.debug("[updateDB]Cluster:{}, MigrationCluster:{}", cluster, migrationClusterTbl);
		
	}
	

	private void updateRedisMaster() {
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
