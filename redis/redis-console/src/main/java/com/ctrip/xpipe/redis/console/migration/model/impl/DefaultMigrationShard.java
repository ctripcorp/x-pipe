package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilderImpl;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult.ShardMigrationResultStatus;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class DefaultMigrationShard extends AbstractObservable implements MigrationShard {
	private Logger logger = LoggerFactory.getLogger(getClass());
	private static Codec coder = Codec.DEFAULT;
	
	private MigrationCluster parent;
	private MigrationShardTbl migrationShard;
	private ShardMigrationResult shardMigrationResult;
	
	private MigrationService migrationService;
	
	private ShardTbl currentShard;
	private Map<Long, DcTbl> dcs;

	private MigrationCommandBuilder commandBuilder;
	
	private Pair<String, Integer> newMasterAddr;

	public DefaultMigrationShard(MigrationCluster parent, MigrationShardTbl migrationShard, ShardTbl currentShard,Map<Long, DcTbl> dcs,
			MigrationService migrationService) {
		this(parent, migrationShard, currentShard, dcs, migrationService, MigrationCommandBuilderImpl.INSTANCE);
	}

	public DefaultMigrationShard(MigrationCluster parent, MigrationShardTbl migrationShard, ShardTbl currentShard,Map<Long, DcTbl> dcs,
								 MigrationService migrationService, MigrationCommandBuilder commandBuilder) {
		this.parent = parent;
		this.migrationShard = migrationShard;

		this.currentShard = currentShard;
		this.dcs = dcs;

		this.migrationService = migrationService;

		shardMigrationResult = new ShardMigrationResult();

		this.commandBuilder = commandBuilder;

		addObserver(parent);
		addObserver(this);
	}

	@Override
	public MigrationShardTbl getMigrationShard() {
		return migrationShard;
	}
	
	@Override
	public ShardMigrationResult getShardMigrationResult() {
		return shardMigrationResult;
	}
	
	@Override
	public ShardTbl getCurrentShard() {
		return currentShard;
	}

	@Override
	public void update(Object args, Observable observable) {
		MigrationShardTbl toUpdate = getMigrationShard();
		toUpdate.setLog(coder.encode(getShardMigrationResult()));
		migrationService.updateMigrationShard(toUpdate);
		
	}
	
	@Override
	public void doCheck() {
		String cluster = parent.getCurrentCluster().getClusterName();
		String shard = currentShard.getShardName();
		String newPrimaryDc = dcs.get(parent.getMigrationCluster().getDestinationDcId()).getDcName();
		
		logger.info("[doCheck]{}-{}-{}", cluster, shard, newPrimaryDc);
		CommandFuture<PrimaryDcCheckMessage> checkResult = commandBuilder.buildDcCheckCommand(cluster, shard, newPrimaryDc, newPrimaryDc).execute();
		checkResult.addListener(new CommandFutureListener<PrimaryDcCheckMessage>() {
			@Override
			public void operationComplete(CommandFuture<PrimaryDcCheckMessage> commandFuture)
					throws Exception {
				try {
					PrimaryDcCheckMessage res = commandFuture.get();
					if(PRIMARY_DC_CHECK_RESULT.SUCCESS.equals(res.getErrorType())){
						shardMigrationResult.updateStepResult(ShardMigrationStep.CHECK, true, "Check success");
					} else {
						shardMigrationResult.updateStepResult(ShardMigrationStep.CHECK, false, res.getErrorMessage());
					}
				} catch (ExecutionException e) {
					logger.error("[doCheck][fail]",e);
					shardMigrationResult.updateStepResult(ShardMigrationStep.CHECK, false, e.getMessage());
				}
				
				notifyObservers(this);
			}
		});
	}
	
	@Override
	public void doMigrate() {
		String cluster = parent.getCurrentCluster().getClusterName();
		String shard = currentShard.getShardName();
		String newPrimaryDc = dcs.get(parent.getMigrationCluster().getDestinationDcId()).getDcName();
		String prevPrimaryDc = dcs.get(parent.getCurrentCluster().getActivedcId()).getDcName();
		
		logger.info("[doMigrate]{}-{}, {}->{}", cluster, shard, prevPrimaryDc, newPrimaryDc);
		try {
			doPrevPrimaryDcMigrate(cluster, shard, prevPrimaryDc).get();
		} catch (InterruptedException | ExecutionException e) {
			shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC, true, "Ignore:" + e.getMessage());
		}
		
		try {
			doNewPrimaryDcMigrate(cluster, shard, newPrimaryDc).get();
		} catch (InterruptedException | ExecutionException e) {
			shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC, false, e.getMessage());
		}
		
		notifyObservers(this);
	}
	
	@Override
	public void doMigrateOtherDc() {
		String cluster = parent.getCurrentCluster().getClusterName();
		String shard = currentShard.getShardName();
		String newPrimaryDc = dcs.get(parent.getMigrationCluster().getDestinationDcId()).getDcName();
		String prevPrimaryDc = dcs.get(parent.getCurrentCluster().getActivedcId()).getDcName();
		
		logger.info("[doMigrateOtherDc]{}-{}, {}->{}", cluster, shard, prevPrimaryDc, newPrimaryDc);
		if(shardMigrationResult.stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
			for(DcTbl dc : dcs.values()) {
				if(!(dc.getDcName().equals(newPrimaryDc))) {
					doOtherDcMigrate(cluster, shard, dc.getDcName(), newPrimaryDc);
				}
			}
		}
		
		if(shardMigrationResult.stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
			shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE, true, "Success");
			shardMigrationResult.setStatus(ShardMigrationResultStatus.SUCCESS);
		} else {
			shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE, false, "Failed");
		}
		if(null != newMasterAddr) {
			updateRedisMaster(newMasterAddr.getLeft(), newMasterAddr.getRight());
		}
		notifyObservers(this);
	}

	@Override
	public void doRollBack() {
		String cluster = parent.getCurrentCluster().getClusterName();
		String shard = currentShard.getShardName();
		String newPrimaryDc = dcs.get(parent.getMigrationCluster().getDestinationDcId()).getDcName();
		String prevPrimaryDc = dcs.get(parent.getCurrentCluster().getActivedcId()).getDcName();

		logger.info("[rollback]{}-{}, {}<-{}", cluster, shard, prevPrimaryDc, newPrimaryDc);
		try {
			doRollBackPrevPrimaryDc(cluster, shard, prevPrimaryDc).get();
		} catch (InterruptedException | ExecutionException e1) {
			logger.error("[rollback][fail]{}-{}, {}<-{}", cluster, shard, prevPrimaryDc, newPrimaryDc);
		}
	}
	
	private CommandFuture<PrimaryDcChangeMessage> doPrevPrimaryDcMigrate(String cluster, String shard, String dc) {
		CommandFuture<PrimaryDcChangeMessage> migrateResult = commandBuilder.buildPrevPrimaryDcCommand(cluster, shard, dc).execute();
		migrateResult.addListener(new CommandFutureListener<PrimaryDcChangeMessage>() {
			@Override
			public void operationComplete(CommandFuture<PrimaryDcChangeMessage> commandFuture) throws Exception {
				try {
					commandFuture.get();
					
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC, true, "Ignored : make previous primary dc read only");
				} catch (Exception e) {
					logger.error("[doPrevPrimaryDcMigrate][fail]",e);
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC, true, "Ignored:" + e.getMessage());
				}
				
				notifyObservers(this);
			}
		});
		return migrateResult;
	}

	private CommandFuture<PrimaryDcChangeMessage> doNewPrimaryDcMigrate(String cluster, String shard, String newPrimaryDc) {
		CommandFuture<PrimaryDcChangeMessage> migrateResult = commandBuilder.buildNewPrimaryDcCommand(cluster, shard, newPrimaryDc).execute();
		migrateResult.addListener(new CommandFutureListener<PrimaryDcChangeMessage>() {
			@Override
			public void operationComplete(CommandFuture<PrimaryDcChangeMessage> commandFuture) throws Exception {
				try {
					PrimaryDcChangeMessage res = commandFuture.get();
					
					if(PRIMARY_DC_CHANGE_RESULT.SUCCESS.equals(res.getErrorType())) {
						shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC, true, res.getErrorMessage());
						newMasterAddr = Pair.of(res.getNewMasterIp(), res.getNewMasterPort());
					} else {
						shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC, false, res.getErrorMessage());
					}
				} catch (Exception e) {
					logger.error("[doNewPrimaryDcMigrate][fail]",e);
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC, false, e.getMessage());
				}
				
				notifyObservers(this);
			}
		});
		return migrateResult;
	}
	
	private CommandFuture<PrimaryDcChangeMessage> doOtherDcMigrate(String cluster, String shard, String dc, String newPrimaryDc) {
		CommandFuture<PrimaryDcChangeMessage> migrateResult = commandBuilder.buildOtherDcCommand(cluster, shard, newPrimaryDc, dc).execute();
		migrateResult.addListener(new CommandFutureListener<PrimaryDcChangeMessage>() {
			@Override
			public void operationComplete(CommandFuture<PrimaryDcChangeMessage> commandFuture) throws Exception {
				try {
					PrimaryDcChangeMessage res = commandFuture.get();
					
					if(PRIMARY_DC_CHANGE_RESULT.SUCCESS.equals(res.getErrorType())) {
						shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_OTHER_DC, true, res.getErrorMessage());
					} else {
						shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_OTHER_DC, false, res.getErrorMessage());
					}
				} catch (Exception e) {
					logger.error("[doOtherDcMigrate][fail]",e);
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_OTHER_DC, false, e.getMessage());
				}
				
				notifyObservers(this);
			}
		});
		return migrateResult;
	}

	private CommandFuture<PrimaryDcChangeMessage> doRollBackPrevPrimaryDc(String cluster, String shard, String dc) {
		CommandFuture<PrimaryDcChangeMessage> migrateResult = commandBuilder.buildRollBackCommand(cluster, shard, dc).execute();
		migrateResult.addListener(new CommandFutureListener<PrimaryDcChangeMessage>() {
			@Override
			public void operationComplete(CommandFuture<PrimaryDcChangeMessage> commandFuture) throws Exception {
				try {
					commandFuture.get();
					logger.info("[doPrevPrimaryDcMigrate][success]");
				} catch (Exception e) {
					logger.error("[doPrevPrimaryDcMigrate][fail]",e);
				}

				notifyObservers(this);
			}
		});
		return migrateResult;
	}

	private void updateRedisMaster(final String ip, final int port) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				List<RedisTbl> toUpdate = new LinkedList<>();
				
				List<RedisTbl> prevDcRedises = parent.getRedisService().findAllByDcClusterShard(dcs.get(parent.getCurrentCluster().getActivedcId()).getDcName(), 
						parent.getCurrentCluster().getClusterName(), getCurrentShard().getShardName());
				for(RedisTbl redis : prevDcRedises) {
					if(redis.isMaster()) {
						redis.setMaster(false);
						toUpdate.add(redis);
					}
				}
				
				List<RedisTbl> newDcRedises = parent.getRedisService().findAllByDcClusterShard(dcs.get(parent.getMigrationCluster().getDestinationDcId()).getDcName(), 
						parent.getCurrentCluster().getClusterName(), getCurrentShard().getShardName());
				for(RedisTbl redis : newDcRedises) {
					if(redis.getRedisIp().equals(ip) && redis.getRedisPort() == port) {
						redis.setMaster(true);
						toUpdate.add(redis);
					}
				}
				
				logger.info("[UpdateMasterTo]{}:{}", ip, port);
				parent.getRedisService().batchUpdate(toUpdate);
				
			}
		}).start();
	}
}
