package com.ctrip.xpipe.redis.console.service.migration;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.MigrationClusterModel;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventModel;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;

public interface MigrationService {
	MigrationEventTbl find(long id);
	List<MigrationEventTbl> findAll();
	MigrationClusterTbl findMigrationCluster(long eventId, long clusterId);
	List<MigrationClusterTbl> findAllMigrationCluster(long clusterId);
	List<MigrationShardTbl> findMigrationShards(long migrationClusterId);
	
	List<MigrationClusterModel> getMigrationClusterModel(long eventId);
	
	void updateMigrationShard(MigrationShardTbl shard);
	void updateMigrationCluster(MigrationClusterTbl cluster);
	
	Long createMigrationEvent(MigrationRequest request);
	void continueMigrationCluster(long eventId, long clusterId);
	void continueMigrationEvent(long id);
	void cancelMigrationCluster(long eventId, long clusterId);
	void rollbackMigrationCluster(long eventId, long clusterId);
	void forcePublishMigrationCluster(long eventId, long clusterId);
	void forceEndMigrationClsuter(long eventId, long clusterId);
}
