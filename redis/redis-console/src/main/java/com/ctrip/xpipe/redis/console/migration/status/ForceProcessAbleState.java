package com.ctrip.xpipe.redis.console.migration.status;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 27, 2017
 */
public interface ForceProcessAbleState extends MigrationState{

    void updateAndForceProcess();

}
