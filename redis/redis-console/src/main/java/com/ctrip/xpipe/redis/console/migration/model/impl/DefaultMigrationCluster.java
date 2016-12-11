package com.ctrip.xpipe.redis.console.migration.model.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationInitiatedStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;

public class DefaultMigrationCluster extends AbstractObservable implements MigrationCluster {
	
	private MigrationStat currentStat;
	private MigrationClusterTbl migrationCluster;
	private List<MigrationShard> migrationShards = new LinkedList<>();

	private ClusterTbl currentCluster;
	private Map<Long, ShardTbl> shards;
	private Map<Long, DcTbl> dcs;
	
	private ClusterService clusterService;
	private ShardService shardService;
	private DcService dcService;
	private MigrationService migrationService;

	public DefaultMigrationCluster(MigrationClusterTbl migrationCluster, DcService dcService, ClusterService clusterService, ShardService shardService,
			MigrationService migrationService) {
		this.migrationCluster = migrationCluster;
		this.currentStat = new MigrationInitiatedStat(this);
		
		this.clusterService = clusterService;
		this.shardService = shardService;
		this.dcService = dcService;
		this.migrationService = migrationService;
		
		loadMetaInfo();
	}

	@Override
	public MigrationStatus getStatus() {
		return currentStat.getStat();
	}

	@Override
	public MigrationClusterTbl getMigrationCluster() {
		return migrationCluster;
	}

	@Override
	public List<MigrationShard> getMigrationShards() {
		return migrationShards;
	}

	@Override
	public ClusterTbl getCurrentCluster() {
		return currentCluster;
	}
	
	@Override
	public Map<Long, ShardTbl> getClusterShards() {
		return shards;
	}
	
	@Override
	public Map<Long, DcTbl> getClusterDcs() {
		return dcs;
	}

	@Override
	public void addNewMigrationShard(MigrationShard migrationShard) {
		migrationShards.add(migrationShard);
	}

	@Override
	public void process() {
		this.currentStat.action();
	}

	@Override
	public void updateStat(MigrationStat stat) {
		this.currentStat = stat;
	}

	@Override
	@DalTransaction
	public void cancel() {
		ClusterTbl cluster = currentCluster;
		cluster.setStatus(ClusterStatus.Normal.toString());
		getClusterService().update(cluster);
		
		MigrationClusterTbl migrationClusterTbl = migrationCluster;
		migrationClusterTbl.setStatus(MigrationStatus.Cancelled.toString());
		migrationClusterTbl.setEndTime(new Date());
		getMigrationService().updateMigrationCluster(migrationClusterTbl);
	}

	@Override
	public void update(Object args, Observable observable) {
		this.currentStat.refresh();
	}

	@Override
	public ClusterService getClusterService() {
		return clusterService;
	}

	@Override
	public ShardService getShardService() {
		return shardService;
	}

	@Override
	public DcService getDcService() {
		return dcService;
	}

	@Override
	public MigrationService getMigrationService() {
		return migrationService;
	}

	private void loadMetaInfo() {
		this.currentCluster = getClusterService().find(migrationCluster.getClusterId());
		this.shards = generateShardMap(getShardService().findAllByClusterName(currentCluster.getClusterName()));
		this.dcs = generateDcMap(getDcService().findClusterRelatedDc(currentCluster.getClusterName()));
	}
	
	private Map<Long, ShardTbl> generateShardMap(List<ShardTbl> shards) {
		Map<Long, ShardTbl> result = new HashMap<>();
		
		for(ShardTbl shard : shards) {
			result.put(shard.getId(), shard);
		}
		
		return result;
	}
	
	private Map<Long, DcTbl> generateDcMap(List<DcTbl> dcs) {
		Map<Long, DcTbl> result = new HashMap<>();
		
		for(DcTbl dc : dcs) {
			result.put(dc.getId(), dc);
		}
		
		return result;
	}

}
