package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;

public class DefaultMigrationShard extends AbstractObservable implements MigrationShard, Observable {
	
	private MigrationShardTbl migrationShard;

	public DefaultMigrationShard(MigrationShardTbl migrationShard) {
		this.migrationShard = migrationShard;
	}
	
	@Override
	public MigrationShardTbl getMigrationShard() {
		return migrationShard;
	}

}
