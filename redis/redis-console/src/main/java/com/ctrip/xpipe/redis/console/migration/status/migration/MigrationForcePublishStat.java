package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
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
public class MigrationForcePublishStat extends AbstractMigrationPublishStat {

	private ExecutorService fixedThreadPool;
	
	public MigrationForcePublishStat(MigrationCluster holder) {
		super(holder, MigrationStatus.ForcePublish);
		this.setNextAfterSuccess(new MigrationForceFailStat(holder))
			.setNextAfterFail(this);
		
		int threadCnt = getHolder().getMigrationShards().size() == 0 ? 1 : getHolder().getMigrationShards().size();
		fixedThreadPool = Executors.newFixedThreadPool(threadCnt, XpipeThreadFactory.create("MigrationForcePublish"));
	}

	@Override
	public void action() {
		doMigrateOtherDc();
		
		updateDB();
		
		if(publish()) {
			updateAndProcess(nextAfterSuccess(), true);
		} else {
			updateAndProcess(nextAfterFail(), false);
		}
	}
	
	private void doMigrateOtherDc(){ 
		CountDownLatch latch = new CountDownLatch(getHolder().getMigrationShards().size());
		for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
			fixedThreadPool.submit(new Runnable() {
				@Override
				public void run() {
					logger.info("[doOtherDcMigrate][start]{},{}",getHolder().getCurrentCluster().getClusterName(), 
							migrationShard.getCurrentShard().getShardName());
					migrationShard.doMigrateOtherDc();
					latch.countDown();
					logger.info("[doOtherDcMigrate][done]{},{}",getHolder().getCurrentCluster().getClusterName(), 
							migrationShard.getCurrentShard().getShardName());
				}
			});
		}
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("[MigrationForcePublish][doMigrateOtherDc][await][fail]",e);
		}
	};

	@DalTransaction
	private void updateDB() {
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setActivedcId(getHolder().getMigrationCluster().getDestinationDcId());
		cluster.setStatus(ClusterStatus.TmpMigrated.toString());
		getHolder().getClusterService().update(cluster);

		MigrationClusterTbl migrationClusterTbl = getHolder().getMigrationCluster();
		migrationClusterTbl.setEndTime(new Date());
		migrationClusterTbl.setStatus(MigrationStatus.ForcePublish.toString());
		getHolder().getMigrationService().updateMigrationCluster(migrationClusterTbl);
		
		logger.info("[updateDB]Cluster:{}, MigrationCluster:{}", cluster, migrationClusterTbl);
	}

}
