package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStat;

/**
 * @author shyin
 *
 *         Dec 11, 2016
 */
public interface MigrationClusterAction {
    void addNewMigrationShard(MigrationShard migrationShard);

    void process();
    void updateStat(MigrationStat stat);
    void cancel();

}
