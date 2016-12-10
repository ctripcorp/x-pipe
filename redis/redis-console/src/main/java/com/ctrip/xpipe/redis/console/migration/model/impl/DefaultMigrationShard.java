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
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class DefaultMigrationShard extends AbstractObservable implements MigrationShard {
	private static Codec coder = Codec.DEFAULT;
	
	private MigrationCluster parent;
	private MigrationShardTbl migrationShard;
	private ShardMigrationResult shardMigrationResult;
	
	private MigrationService migrationService;
	
	private ShardTbl currentShard;
	private Map<Long, DcTbl> dcs;

	private MigrationCommandBuilder commandBuilder;

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
		
		CommandFuture<PrimaryDcCheckMessage> checkResult = commandBuilder.buildDcCheckCommand(cluster, shard, newPrimaryDc, newPrimaryDc).execute();
		checkResult.addListener(new CommandFutureListener<PrimaryDcCheckMessage>() {
			@Override
			public void operationComplete(CommandFuture<PrimaryDcCheckMessage> commandFuture)
					throws Exception {
				PrimaryDcCheckMessage res = commandFuture.get();
				if(res.getErrorType().equals(PRIMARY_DC_CHECK_RESULT.SUCCESS)) {
					shardMigrationResult.updateStepResult(ShardMigrationStep.CHECK, true, res.getErrorMessage());
				} else {
					shardMigrationResult.updateStepResult(ShardMigrationStep.CHECK, false, res.getErrorMessage());
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
		
		try {
			doPrevPrimaryDcMigrate(cluster, shard, prevPrimaryDc, newPrimaryDc).get();
		} catch (InterruptedException | ExecutionException e) {
			shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC, true, "Ignore with fail.");
		}
		
		try {
			doNewPrimaryDcMigrate(cluster, shard, newPrimaryDc).get();
		} catch (InterruptedException | ExecutionException e) {
			shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC, false, "Cannot fetch migrate result");
		}
		
		for(DcTbl dc : dcs.values()) {
			if(!(dc.getDcName().equals(newPrimaryDc)) && !(dc.getDcName().equals(prevPrimaryDc))) {
				doOtherDcMigrate(cluster, shard, dc.getDcName(), newPrimaryDc);
			}
		}
		
		if(shardMigrationResult.stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
			shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE, true, "Success");
			shardMigrationResult.setStatus(ShardMigrationResultStatus.SUCCESS);
		} else {
			shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE, false, "Failed");
		}
		
		notifyObservers(this);
	}
	
	private CommandFuture<PrimaryDcChangeMessage> doPrevPrimaryDcMigrate(String cluster, String shard, String dc, String newPrimaryDc) {
		CommandFuture<PrimaryDcChangeMessage> migrateResult = commandBuilder.buildPrevPrimaryDcCommand(cluster, shard, dc).execute();
		migrateResult.addListener(new CommandFutureListener<PrimaryDcChangeMessage>() {
			@Override
			public void operationComplete(CommandFuture<PrimaryDcChangeMessage> commandFuture) throws Exception {
				PrimaryDcChangeMessage res = commandFuture.get();
				if(res.getErrorType().equals(PRIMARY_DC_CHANGE_RESULT.SUCCESS)) {
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC, true, res.getErrorMessage());
				} else {
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC, true, res.getErrorMessage());
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
				PrimaryDcChangeMessage res = commandFuture.get();
				if(res.getErrorType().equals(PRIMARY_DC_CHANGE_RESULT.SUCCESS)) {
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC, true, res.getErrorMessage());
				} else {
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC, false, res.getErrorMessage());
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
				PrimaryDcChangeMessage res = commandFuture.get();
				if(res.getErrorType().equals(PRIMARY_DC_CHANGE_RESULT.SUCCESS)) {
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_OTHER_DC, true, res.getErrorMessage());
				} else {
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_OTHER_DC, true, res.getErrorMessage());
				}
				
				notifyObservers(this);
			}
		});
		return migrateResult;
	}
}
