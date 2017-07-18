package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.redis.console.migration.status.MigrationState;

/**
 * @author shyin
 *
 *         Dec 11, 2016
 */
public interface MigrationClusterAction {

    void addNewMigrationShard(MigrationShard migrationShard);

    void process();
    void updateStat(MigrationState stat);
    void cancel();
    void rollback();
    void forcePublish();
    void forceEnd();

}
