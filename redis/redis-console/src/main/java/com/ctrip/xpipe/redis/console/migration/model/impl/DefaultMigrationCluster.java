package com.ctrip.xpipe.redis.console.migration.model.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationCancelledStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationCheckingStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationForceFailStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationForcePublishStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationInitiatedStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationMigratingStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPartialSuccessStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPublishStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStatus;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationSuccessStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationTmpEndStat;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
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
		setStatus();
		
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

	private void setStatus() {
		String status = this.migrationCluster.getStatus();
		if(MigrationStatus.isSameStatus(status, MigrationStatus.Initiated)) {
			this.currentStat = new MigrationInitiatedStat(this);
		} else if (MigrationStatus.isSameStatus(status, MigrationStatus.Checking)) {
			this.currentStat = new MigrationCheckingStat(this);
		} else if (MigrationStatus.isSameStatus(status, MigrationStatus.Migrating)) {
			this.currentStat = new MigrationMigratingStat(this);
		} else if (MigrationStatus.isSameStatus(status, MigrationStatus.Publish)) {
			this.currentStat = new MigrationPublishStat(this);
		} else if (MigrationStatus.isSameStatus(status, MigrationStatus.Success)) {
			this.currentStat = new MigrationSuccessStat(this);
		} else if (MigrationStatus.isSameStatus(status, MigrationStatus.Cancelled)) {
			this.currentStat = new MigrationCancelledStat(this);
		} else if (MigrationStatus.isSameStatus(status, MigrationStatus.PartialSuccess)) {
			this.currentStat = new MigrationPartialSuccessStat(this);
		} else if (MigrationStatus.isSameStatus(status, MigrationStatus.ForcePublish)) {
			this.currentStat = new MigrationForcePublishStat(this);
		} else if (MigrationStatus.isSameStatus(status, MigrationStatus.TmpEnd)) {
			this.currentStat = new MigrationTmpEndStat(this);
		} else if (MigrationStatus.isSameStatus(status, MigrationStatus.ForceFail)) {
			this.currentStat = new MigrationForceFailStat(this);
		} else {
			this.currentStat = new MigrationInitiatedStat(this);
		}
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
