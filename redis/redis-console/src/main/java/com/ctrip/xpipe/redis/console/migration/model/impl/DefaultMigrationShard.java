package com.ctrip.xpipe.redis.console.migration.model.impl;

import java.util.Map;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;

public class DefaultMigrationShard extends AbstractObservable implements MigrationShard, Observable, Observer {
	private static Codec coder = Codec.DEFAULT;
	
	private MigrationCluster parent;
	private MigrationShardTbl migrationShard;
	private ShardMigrationResult shardMigrationResult;
	
	private MigrationService migrationService;
	
	private ShardTbl currentShard;
	private Map<Long, DcTbl> dcs;

	public DefaultMigrationShard(MigrationCluster parent, MigrationShardTbl migrationShard, ShardTbl currentShard,Map<Long, DcTbl> dcs,
			MigrationService migrationService) {
		this.parent = parent;
		this.migrationShard = migrationShard;
		
		this.currentShard = currentShard;
		this.dcs = dcs;
		
		this.migrationService = migrationService;
		
		shardMigrationResult = new ShardMigrationResult();
		
		addObserver((Observer) parent);
		addObserver((Observer) this);
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
	public void doCheck() {
		String cluster = parent.getCurrentCluster().getClusterName();
		String shard = currentShard.getShardName();
		String newPrimaryDc = dcs.get(parent.getMigrationCluster().getDestinationDcId()).getDcName();
		
		CommandFuture<PrimaryDcCheckMessage> checkResult = MigrationCommandBuilder.INSTANCE.buildDcCheckCommand(cluster, shard, newPrimaryDc, newPrimaryDc).execute();
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
		// TODO
		
		notifyObservers(this);
	}

	@Override
	public void update(Object args, Observable observable) {
		if(args instanceof MigrationShard) {
			MigrationShard migrationShard = (MigrationShard)args;
			MigrationShardTbl toUpdate = migrationShard.getMigrationShard();
			toUpdate.setLog(coder.encode(migrationShard.getShardMigrationResult()));
			migrationService.updateMigrationShard(toUpdate);
		}
	}

}
