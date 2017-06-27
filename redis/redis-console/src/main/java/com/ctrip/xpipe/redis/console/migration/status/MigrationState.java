package com.ctrip.xpipe.redis.console.migration.status;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public interface MigrationState {

    MigrationStatus getStatus();

    void rollback();

    void action();

    void refresh();

    MigrationState nextAfterSuccess();

    MigrationState nextAfterFail();

}
