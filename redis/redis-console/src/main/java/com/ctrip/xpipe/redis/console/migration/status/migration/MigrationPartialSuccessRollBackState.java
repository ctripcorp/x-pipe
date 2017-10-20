package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.utils.LogUtils;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationPartialSuccessRollBackState extends AbstractMigrationState {

    public MigrationPartialSuccessRollBackState(MigrationCluster holder) {
        super(holder, MigrationStatus.RollBack);
        this.setNextAfterSuccess(new MigrationAbortedState(holder))
                .setNextAfterFail(this);
    }

    @Override
    protected void doRollback() {
        throw new UnsupportedOperationException("already rollbacking, can not tryRollback tryRollback");
    }

    @Override
    public void doAction() {
    	
    	CountDownLatch latch = new CountDownLatch(getHolder().getMigrationShards().size());
    	StringBuilder errorMessage = new StringBuilder();
    	
        for(MigrationShard migrationShard : getHolder().getMigrationShards()) {

            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
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
			latch.await(migrationWaitTimeMilli, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			logger.error("[MigrationRollBackStat][await][shard][doRollBack][fail]",e);
			errorMessage.append(LogUtils.error(String.format("[wait time exceed]%s ms", migrationWaitTimeMilli)));
		}
        
        String error = errorMessage.toString();
        if(StringUtil.isEmpty(error)){
        	updateAndProcess(nextAfterSuccess());
        }else{
        	updateAndStop(nextAfterFail());
        }
    }

    @Override
    public void refresh() {
        // Nothing to do
        logger.debug("[MigrationRollBack]{}", getHolder().clusterName());
    }
}
