package com.ctrip.xpipe.redis.console.migration.model;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 28, 2017
 */
public enum ShardMigrationStep {
    CHECK,
    MIGRATE_PREVIOUS_PRIMARY_DC,
    MIGRATE_NEW_PRIMARY_DC,
    MIGRATE_OTHER_DC,
    MIGRATE

}
