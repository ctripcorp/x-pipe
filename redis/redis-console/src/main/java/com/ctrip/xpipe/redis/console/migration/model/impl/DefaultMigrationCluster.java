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

public class DefaultMigrationCluster extends AbstractObservable implements MigrationCluster, Observer, Observable {
	
	private MigrationStat currentStat;
	private MigrationClusterTbl migrationCluster;
	private List<MigrationShard> migrationShards = new LinkedList<>();
	
	private ClusterService clusterService;
	private MigrationService migrationService;
	
	private ClusterTbl currentCluster;
	private Map<Long, ShardTbl> shards;
	private Map<Long, DcTbl> dcs;

	public DefaultMigrationCluster(MigrationClusterTbl migrationCluster, DcService dcService, ClusterService clusterService, ShardService shardService,
			MigrationService migrationService) {
		this.migrationCluster = migrationCluster;
		this.currentStat = new MigrationInitiatedStat(this);
		
		this.clusterService = clusterService;
		this.migrationService = migrationService;
		
		loadMetaInfo(dcService, clusterService, shardService);
	}

	public DefaultMigrationCluster(MigrationClusterTbl migrationCluster, MigrationStat stat, DcService dcService, ClusterService clusterService, ShardService shardService,
			MigrationService migrationService) {
		this.migrationCluster = migrationCluster;
		this.currentStat = stat;
		
		this.clusterService = clusterService;
		this.migrationService = migrationService;
		
		loadMetaInfo(dcService, clusterService, shardService);
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
	public void updateMigrationCluster(MigrationClusterTbl migrationCluster) {
		migrationService.updateMigrationCluster(migrationCluster);
	}
	
	@Override
	public ClusterTbl getCurrentCluster() {
		return currentCluster;
	}
	
	@Override
	public void updateCurrentCluster(ClusterTbl cluster) {
		if( cluster.getId() == currentCluster.getId()) {
			clusterService.update(cluster);
		}
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
	public List<MigrationShard> getMigrationShards() {
		return migrationShards;
	}
	
	@Override
	public void addNewMigrationShard(MigrationShard migrationShard) {
		migrationShards.add(migrationShard);
	}

	@Override
	public void update(Object args, Observable observable) {
		if(args instanceof MigrationShard) {
			this.currentStat.refresh();
		}
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
	public void publishStatus(ClusterStatus status, MigrationStatus migrationStatus) {
		ClusterTbl cluster = currentCluster;
		cluster.setStatus(status.toString());
		cluster.setActivedcId(migrationCluster.getDestinationDcId());
		clusterService.update(cluster);
		
		MigrationClusterTbl migrationClusterTbl = migrationCluster;
		migrationClusterTbl.setStatus(migrationStatus.toString());
		migrationService.updateMigrationCluster(migrationClusterTbl);
	}
	
	@Override
	@DalTransaction
	public void cancel() {
		ClusterTbl cluster = currentCluster;
		cluster.setStatus(ClusterStatus.Normal.toString());
		clusterService.update(cluster);
		
		MigrationClusterTbl migrationClusterTbl = migrationCluster;
		migrationClusterTbl.setStatus(MigrationStatus.Cancelled.toString());
		migrationClusterTbl.setEndTime(new Date());
		migrationService.updateMigrationCluster(migrationClusterTbl);
	}
	
	private void loadMetaInfo(DcService dcService, ClusterService clusterService, ShardService shardService) {
		this.currentCluster = clusterService.find(migrationCluster.getClusterId());
		this.shards = generateShardMap(shardService.findAllByClusterName(currentCluster.getClusterName()));
		this.dcs = generateDcMap(dcService.findClusterRelatedDc(currentCluster.getClusterName()));
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
