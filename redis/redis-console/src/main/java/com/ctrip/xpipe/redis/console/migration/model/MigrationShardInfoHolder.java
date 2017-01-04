package com.ctrip.xpipe.redis.console.migration.model;

import java.net.InetSocketAddress;

import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult;
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

    InetSocketAddress getNewMasterAddress();
}
