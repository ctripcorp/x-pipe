package com.ctrip.xpipe.redis.console.migration.model;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationShardAction {
    void doCheck();
    void doMigrate();
    void doMigrateOtherDc();
    void doRollBack();
}
