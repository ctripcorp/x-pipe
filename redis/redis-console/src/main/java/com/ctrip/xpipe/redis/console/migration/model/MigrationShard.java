package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.api.observer.Observer;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationShard extends MigrationShardInfoHolder, MigrationShardAction, Observer{

    String shardName();

    ShardMigrationStepResult stepResult(ShardMigrationStep step);

    void retry(ShardMigrationStep step);

    void markCheckFail(String failMessage);
}
