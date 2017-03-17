package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.utils.LogUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationRollBackState extends AbstractMigrationState {

    private ExecutorService cachedThreadPool = Executors.newCachedThreadPool(XpipeThreadFactory.create("MigrationRollBack"));

    public MigrationRollBackState(MigrationCluster holder) {
        super(holder, MigrationStatus.RollBack);
        this.setNextAfterSuccess(new MigrationAbortedState(holder))
                .setNextAfterFail(this);
    }

    @Override
    public void action() {
    	
    	CountDownLatch latch = new CountDownLatch(getHolder().getMigrationShards().size());
    	StringBuilder errorMessage = new StringBuilder();
    	
        for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
            cachedThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                	try{
                		migrationShard.doRollBack();
                	}catch(Exception e){
                		logger.error("[run]" + migrationShard, e);
                		errorMessage.append(LogUtils.error(String.format("%s", migrationShard, e.toString())));
                	}finally{
                       latch.countDown();
                	}
                }
            });
        }
        try {
			latch.await(migrationWaitTimeSeconds, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.error("[MigrationRollBackStat][await][shard][doRollBack][fail]",e);
			errorMessage.append(LogUtils.error(String.format("[wait time exceed]%s seconds", migrationWaitTimeSeconds)));
		}
        
        String error = errorMessage.toString();
        if(StringUtil.isEmpty(error)){
        	updateAndProcess(nextAfterSuccess(), true);
        }else{
        	updateAndProcess(nextAfterFail(), false);
        }
    }

    @Override
    public void refresh() {
        // Nothing to do
        logger.debug("[MigrationRollBack]{}", getHolder().getCurrentCluster().getClusterName());
    }
}
