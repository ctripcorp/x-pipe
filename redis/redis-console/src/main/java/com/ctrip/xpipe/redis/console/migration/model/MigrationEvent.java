package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationEvent {
	MigrationEventTbl getEvent();
	MigrationCluster getMigrationCluster(long clusterId);
	
	void addMigrationCluster(MigrationCluster migrationCluster);
	
}
