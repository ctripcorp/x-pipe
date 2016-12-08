package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult.ShardMigrationResultStatus;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

public class MigrationPartialSuccessStat extends AbstractMigrationStat implements MigrationStat {
	
	private ExecutorService cachedThreadPool;
	
	public MigrationPartialSuccessStat(MigrationCluster holder) {
		super(holder, MigrationStatus.PartialSuccess);
		this.setNextAfterSuccess(new MigrationPublishStat(getHolder()))
			.setNextAfterFail(this);
		
		cachedThreadPool = Executors.newCachedThreadPool(XpipeThreadFactory.create("MigrationPartialSuccess"));
	}

	@Override
	public void action() {
		// Update cluster status
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.Migrating.toString());
		getHolder().updateCurrentCluster(cluster);
		
		for(final MigrationShard shard : getHolder().getMigrationShards()) {
			if(!shard.getShardMigrationResult().getResult().equals(ShardMigrationResultStatus.SUCCESS)) {
				cachedThreadPool.submit(new Runnable() {

					@Override
					public void run() {
						shard.doMigrate();
					}
				});
			}
		}
	}
	
	@Override
	public void refresh() {
		int successCnt = 0;
		int currentWorkingCnt = 0;
		
		for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
			if(migrationShard.getShardMigrationResult().stepTerminated(ShardMigrationStep.MIGRATE)) {
				if(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE)) {
					++successCnt;
				}
			} else {
				++currentWorkingCnt;
			}
		}
		
		if(currentWorkingCnt == 0) {
			if (successCnt == getHolder().getMigrationShards().size()) {
				updateAndProcess(nextAfterSuccess(), true);
			} else {
				updateAndProcess(nextAfterFail(), false);
			}
		} else {
			// Still migrating , Nothing to do
		}
	}

}
