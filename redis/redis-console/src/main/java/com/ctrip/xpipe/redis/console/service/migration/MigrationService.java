package com.ctrip.xpipe.redis.console.service.migration;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventModel;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;

public interface MigrationService {
	MigrationEventTbl find(long id);
	List<MigrationEventTbl> findAll();
	MigrationClusterTbl findMigrationCluster(long eventId, long clusterId);
	List<MigrationShardTbl> findMigrationShards(long migrationClusterId);
	
	long createMigrationEvent(MigrationEventModel events);
	void continueMigrationEvent(long id);
	void cancelMigrationEvent(long id);
}
