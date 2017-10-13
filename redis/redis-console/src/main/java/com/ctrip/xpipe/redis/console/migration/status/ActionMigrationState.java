package com.ctrip.xpipe.redis.console.migration.status;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 31, 2017
 */
public interface ActionMigrationState extends MigrationState{

    void rollback();

    void action();

    void checkTimeout();

    void cancelCheckTimeout();

}
