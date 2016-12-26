package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationMigratingStat extends AbstractMigrationMigratingStat {

	private ExecutorService fixedThreadPool;
	
	public MigrationMigratingStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Migrating);
		this.setNextAfterSuccess(new MigrationPublishStat(getHolder()))
			.setNextAfterFail(new MigrationPartialSuccessStat(getHolder()));
		
		int threadSize = holder.getMigrationShards().size() == 0 ? 1 : holder.getMigrationShards().size();
		fixedThreadPool = Executors.newFixedThreadPool(threadSize, 
					XpipeThreadFactory.create("MigrationMigrating"));
	}

	@Override
	public void action() {
		updateDB();
		
		for(final MigrationShard shard : getHolder().getMigrationShards()) {
			fixedThreadPool.submit(new Runnable() {
				
				@Override
				public void run() {
					shard.doMigrate();
				}
			});
		}
	}
	
	@DalTransaction
	private void updateDB() {
		// Update cluster status
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.Migrating.toString());
		getHolder().getClusterService().update(cluster);
		
		// Update migration cluster status
		MigrationClusterTbl migrationCluster = getHolder().getMigrationCluster();
		migrationCluster.setStatus(MigrationStatus.Migrating.toString());
		getHolder().getMigrationService().updateMigrationCluster(migrationCluster);
	}
	
}
