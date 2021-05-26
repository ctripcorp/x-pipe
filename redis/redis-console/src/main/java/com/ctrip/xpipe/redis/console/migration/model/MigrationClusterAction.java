package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.console.migration.status.MigrationState;

/**
 * @author shyin
 *
 *         Dec 11, 2016
 */
public interface MigrationClusterAction extends Startable, Stoppable {

    void addNewMigrationShard(MigrationShard migrationShard);

    void process();
    void updateStat(MigrationState stat);
    void cancel();
    void rollback();
    void forceProcess();
    void forceEnd();
    boolean isStarted();

}
