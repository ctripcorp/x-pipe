package com.ctrip.xpipe.redis.console.migration.model.impl;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ctrip.xpipe.redis.console.migration.status.migration.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class DefaultMigrationCluster extends AbstractObservable implements MigrationCluster {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private MigrationStat currentStat;
	private MigrationClusterTbl migrationCluster;
	private List<MigrationShard> migrationShards = new LinkedList<>();

	private ClusterTbl currentCluster;
	private Map<Long, ShardTbl> shards;
	private Map<Long, DcTbl> dcs;
	
	private ClusterService clusterService;
	private ShardService shardService;
	private DcService dcService;
	private RedisService redisService;
	private MigrationService migrationService;

	public DefaultMigrationCluster(MigrationClusterTbl migrationCluster, DcService dcService, ClusterService clusterService, ShardService shardService,
			RedisService redisService,MigrationService migrationService) {
		this.migrationCluster = migrationCluster;
		setStatus();
		
		this.clusterService = clusterService;
		this.shardService = shardService;
		this.dcService = dcService;
		this.redisService = redisService;
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
		logger.info("[Process]{}-{}, {}", migrationCluster.getEventId(),getCurrentCluster().getClusterName(), this.currentStat.getStat());
		this.currentStat.action();
	}

	@Override
	public void updateStat(MigrationStat stat) {
		logger.info("[UpdateStat]{}-{}, {} -> {}",
				migrationCluster.getEventId(), getCurrentCluster().getClusterName(), this.currentStat.getStat(), stat.getStat());
		this.currentStat = stat;
	}

	@Override
	public void cancel() {
		logger.info("[Cancel]{}-{}, {} -> Cancelled", migrationCluster.getEventId(), getCurrentCluster().getClusterName(), this.currentStat.getStat());
		if(!currentStat.getStat().equals(MigrationStatus.Initiated)
				&& !currentStat.getStat().equals(MigrationStatus.Checking)) {
			throw new IllegalStateException(String.format("Cannot cancel while %s", this.currentStat.getStat()));
		}
		MigrationStat cancelStat = new MigrationCancelledStat(this);
		cancelStat.action();
	}

	@Override
	public void rollback() {
		logger.info("[Rollback]{}-{}, {} -> Rollback", migrationCluster.getEventId(), getCurrentCluster().getClusterName(), this.currentStat.getStat());
		if(!currentStat.getStat().equals(MigrationStatus.PartialSuccess)) {
			throw new IllegalStateException(String.format("Cannot rollback while %s", this.currentStat.getStat()));
		}
		MigrationStat rollBackStat = new MigrationRollBackStat(this);
		rollBackStat.action();
	}
	
	@Override
	public void forcePublish() {
		// TODO : force publish
	}
	
	@Override
	public void forceEnd() {
		// TODO : force end
	}

	@Override
	public void update(Object args, Observable observable) {
		this.currentStat.refresh();
		notifyObservers(this);
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
	public RedisService getRedisService() {
		return redisService;
	}
	
	@Override
	public MigrationService getMigrationService() {
		return migrationService;
	}

	private void setStatus() {
		MigrationStatus status = MigrationStatus.valueOf(migrationCluster.getStatus());
		switch(status) {
		case Initiated :
			currentStat = new MigrationInitiatedStat(this);
			break;
		case Cancelled:
			currentStat = new MigrationCancelledStat(this);
			break;
		case Checking:
			currentStat = new MigrationCheckingStat(this);
			break;
		case ForceFail:
			currentStat = new MigrationForceFailStat(this);
			break;
		case ForcePublish:
			currentStat = new MigrationForcePublishStat(this);
			break;
		case Migrating:
			currentStat = new MigrationMigratingStat(this);
			break;
		case PartialSuccess:
			currentStat = new MigrationPartialSuccessStat(this);
			break;
		case Publish:
			currentStat = new MigrationPublishStat(this);
			break;
		case RollBack:
			currentStat = new MigrationRollBackStat(this);
			break;
		case ForceEnd:
			currentStat = new MigrationForceEndStat(this);
			break;
		case Success:
			currentStat = new MigrationSuccessStat(this);
			break;
		default:
			currentStat = new MigrationInitiatedStat(this);
			break;
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
