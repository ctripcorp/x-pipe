package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.migration.status.PublishState;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationPublishState extends AbstractMigrationPublishState implements PublishState{
	
	public MigrationPublishState(MigrationCluster holder) {
		super(holder, MigrationStatus.Publish);
		this.setNextAfterSuccess(new MigrationSuccessState(getHolder()))
			.setNextAfterFail(this);
	}

	@Override
	protected void doRollback() {
		throw new UnsupportedOperationException("[doRollback]" +
				"[xpipe succeed, publish results to redis client fail, can not rollback, find DBA to manually solve this problem]eventId:" + getHolder().getMigrationEvent().getMigrationEventId());
	}

	@Override
	public void doAction() {

		updateActiveDcIdToDestDcId();

		try {
			logger.info("[action][updateRedisMaster]{}", this);
			updateRedisMaster();
		} catch (ResourceNotFoundException e) {
			logger.error("[action]", e);
		}

		if(publish()) {
			updateAndProcess(nextAfterSuccess());
		} else {
			updateAndStop(nextAfterFail());
		}
	}

	private void updateActiveDcIdToDestDcId() {
		logger.info("[updateActiveDcIdToDestDcId]{}", this);
		getHolder().updateActiveDcIdToDestDcId();
	}
	

	private void updateRedisMaster() throws ResourceNotFoundException {
		List<RedisTbl> toUpdate = new LinkedList<>();
		
		MigrationCluster migrationCluster = getHolder();

		RedisService redisService = migrationCluster.getRedisService();
		String fromDc = migrationCluster.fromDc();
		String destDc = migrationCluster.destDc();
		String clusterName = migrationCluster.clusterName();

		List<RedisTbl> prevDcRedises = redisService.findAllRedisesByDcClusterName(fromDc, clusterName);
		for(RedisTbl redis : prevDcRedises) {
			if(redis.isMaster()) {
				redis.setMaster(false);
				toUpdate.add(redis);
			}
		}

		List<RedisTbl> newDcRedises = redisService.findAllRedisesByDcClusterName(destDc, clusterName);

		for(InetSocketAddress newMasterAddress : getNewMasters()){
			for(RedisTbl redis : newDcRedises) {
				if(redis.getRedisIp().equals(newMasterAddress.getHostString()) && redis.getRedisPort() == newMasterAddress.getPort()) {
					redis.setMaster(true);
					toUpdate.add(redis);
				}
			}
		}
		
		logger.info("[UpdateMaster]{}", toUpdate);
		migrationCluster.getRedisService().updateBatchMaster(toUpdate);
	}

	@Override
	public void forceEnd() {
		updateAndProcess(new MigrationForceEndState(getHolder()));
	}
}
