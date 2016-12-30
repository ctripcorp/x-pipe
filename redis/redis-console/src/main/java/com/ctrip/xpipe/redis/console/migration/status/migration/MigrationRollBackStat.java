package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Chris on 29/12/2016.
 */
public class MigrationRollBackStat extends AbstractMigrationStat implements MigrationStat {

    private ExecutorService cachedThreadPool = Executors.newCachedThreadPool(XpipeThreadFactory.create("MigrationRollBack"));

    public MigrationRollBackStat(MigrationCluster holder) {
        super(holder, MigrationStatus.RollBack);
        this.setNextAfterSuccess(new MigrationCancelledStat(holder))
                .setNextAfterFail(this);
    }

    @Override
    public void action() {
    	CountDownLatch latch = new CountDownLatch(getHolder().getMigrationShards().size());
        for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
            cachedThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    migrationShard.doRollBack();
                    latch.countDown();
                }
            });
        }
        try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("[MigrationRollBackStat][await][shard][doRollBack][fail]",e);
		}
        updateAndProcess(nextAfterSuccess(), true);
    }

    @Override
    public void refresh() {
        // Nothing to do
        logger.info("[MigrationRollBack]{}", getHolder().getCurrentCluster().getClusterName());
    }
}
