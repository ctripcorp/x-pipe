package com.ctrip.xpipe.redis.console.migration.model;

/**
 * Created by Chris on 10/12/2016.
 */
public interface MigrationShardAction {
    void doCheck();
    void doMigrate();

}
