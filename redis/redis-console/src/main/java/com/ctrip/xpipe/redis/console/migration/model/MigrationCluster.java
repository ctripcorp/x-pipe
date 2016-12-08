package com.ctrip.xpipe.redis.console.migration.model;

import java.util.List;

import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;

public interface MigrationCluster {
	MigrationStatus getStatus();
	
	MigrationClusterTbl getMigrationCluster();
	List<MigrationShard> getMigrationShards();
	List<MigrationShard> getCurrentlyWorkingMigrationShards();
	List<MigrationShard> getTerminatedMigrationShards();
	
	void addNewMigrationShard(MigrationShard migrationShard);
	void terminateMigrationShard(MigrationShard migrationShard);
	
	void process();
	void updateStat(MigrationStat stat);
}
