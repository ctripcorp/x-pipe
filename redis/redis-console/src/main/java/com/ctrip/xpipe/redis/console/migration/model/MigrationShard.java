package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;

public interface MigrationShard {
	MigrationShardTbl getMigrationShard();
	ShardMigrationResult getShardMigrationResult();
	
	ShardTbl getCurrentShard();
	
	void doCheck();
	void doMigrate();
}
