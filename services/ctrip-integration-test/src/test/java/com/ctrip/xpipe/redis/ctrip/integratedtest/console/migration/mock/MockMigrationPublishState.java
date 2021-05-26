package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPublishState;

import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2021/3/29
 */
public class MockMigrationPublishState extends MigrationPublishState {

    public MockMigrationPublishState(MigrationCluster holder) {
        super(holder);
    }

    @Override
    public void doAction() {
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            public void doRun() {
                getHolder().getClusterService().updateActivedcId(getHolder().getCurrentCluster().getId(), getHolder().destDcId());
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (Exception e) {
                    logger.info("[doAction] sleep fail", e);
                }

                updateAndProcess(nextAfterSuccess());
            }
        });
    }


}
