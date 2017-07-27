package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.redis.console.migration.model.impl.ShardMigrationException;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationShardAction {
    void doCheck();
    void doMigrate();
    void doMigrateOtherDc();
    void doRollBack() throws ShardMigrationException;
}
