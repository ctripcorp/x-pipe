package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;

public interface MigrationShard {
	MigrationShardTbl getMigrationShard();
}
