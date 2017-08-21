package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationShardInfoHolder {

    MigrationShardTbl getMigrationShard();

    ShardMigrationResult getShardMigrationResult();

    ShardTbl getCurrentShard();

    HostPort getNewMasterAddress();
}
