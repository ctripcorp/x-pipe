package com.ctrip.xpipe.redis.console.migration.model;

import java.util.List;
import java.util.Map;

import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;

public interface MigrationCluster {
	MigrationStatus getStatus();
	MigrationClusterTbl getMigrationCluster();
	List<MigrationShard> getMigrationShards();
	
	ClusterTbl getCurrentCluster();
	void updateCurrentCluster(ClusterTbl cluster);
	Map<Long, ShardTbl> getClusterShards();
	Map<Long, DcTbl> getClusterDcs();
	
	void addNewMigrationShard(MigrationShard migrationShard);
	
	void process();
	void updateStat(MigrationStat stat);
}
