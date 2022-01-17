package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilderImpl;
import com.ctrip.xpipe.redis.console.migration.model.*;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import com.ctrip.xpipe.utils.LogUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class DefaultMigrationShard extends AbstractObservable implements MigrationShard {
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	@SuppressWarnings("unused")
	private MigrationCluster parent;
	private MigrationShardTbl migrationShard;
	private ShardMigrationResult shardMigrationResult;
	
	private MigrationService migrationService;
	
	private ShardTbl currentShard;
	private Map<Long, DcTbl> dcs;

	private MigrationCommandBuilder commandBuilder;

	private String cluster;
	private String shard;
	private String newPrimaryDc;
	private String prevPrimaryDc;

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
		this.shardMigrationResult = DefaultShardMigrationResult.fromEncodeStr(migrationShard.getLog());
		this.commandBuilder = commandBuilder;

		cluster = parent.clusterName();
		shard = currentShard.getShardName();
		newPrimaryDc = dcs.get(parent.getMigrationCluster().getDestinationDcId()).getDcName();
		prevPrimaryDc = dcs.get(parent.getCurrentCluster().getActivedcId()).getDcName();

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

	//for unit test
	public void updateShardMigrationResult(ShardMigrationResult result){
		this.shardMigrationResult = result;
	}
	
	@Override
	public ShardTbl getCurrentShard() {
		return currentShard;
	}
	
	@Override
	public HostPort getNewMasterAddress() {
		return shardMigrationResult.getNewMaster();
	}

	@Override
	public void update(Object args, Observable observable) {

		logger.debug("[update][begin]{}", this);
		long begin = System.currentTimeMillis();

		MigrationShardTbl toUpdate = getMigrationShard();
		String log = getShardMigrationResult().encode();
		migrationService.updateMigrationShardLogById(toUpdate.getId(), log);

		logger.debug("[debug][end]{}", this);
		long end = System.currentTimeMillis();
		if((end - begin) > 3){
			logger.debug("[update][time long]{}, {}", end - begin, this);
		}
	}

	@Override
	public void markCheckFail(String failMessage) {

		shardMigrationResult.updateStepResult(ShardMigrationStep.CHECK, false, LogUtils.error(failMessage));
		notifyObservers(new ShardObserverEvent(shardName(), ShardMigrationStep.CHECK));
	}

	@Override
	public void doCheck() {
		
		logger.info("[doCheck]{}-{}-{}", cluster, shard, newPrimaryDc);
		CommandFuture<PrimaryDcCheckMessage> checkResult = commandBuilder.buildDcCheckCommand(cluster, shard, newPrimaryDc, newPrimaryDc).execute();
		checkResult.addListener(new CommandFutureListener<PrimaryDcCheckMessage>() {
			@Override
			public void operationComplete(CommandFuture<PrimaryDcCheckMessage> commandFuture)
					throws Exception {

				if (commandFuture.isSuccess()) {
					PrimaryDcCheckMessage res = commandFuture.get();
					logger.info("[doCheck]{}, {}, {}, {}", cluster, shard, newPrimaryDc, res);
					if(PRIMARY_DC_CHECK_RESULT.SUCCESS.equals(res.getErrorType())){
						shardMigrationResult.updateStepResult(ShardMigrationStep.CHECK, true, LogUtils.info("Check success"));
					} else if (PRIMARY_DC_CHECK_RESULT.PRIMARY_DC_ALREADY_IS_NEW.equals(res.getErrorType())) {
						shardMigrationResult.updateStepResult(ShardMigrationStep.CHECK, true, LogUtils.info("Already primary dc"));
					} else {
						shardMigrationResult.updateStepResult(ShardMigrationStep.CHECK, false, LogUtils.error(res.getErrorMessage()));
					}
				} else {
					logger.error("[doCheck][fail]{}, {}, {}", cluster, shard, newPrimaryDc, commandFuture.cause());
					shardMigrationResult.updateStepResult(ShardMigrationStep.CHECK, false, LogUtils.error(commandFuture.cause().getMessage()));
				}
				
				notifyObservers(new ShardObserverEvent(shardName(), ShardMigrationStep.CHECK));
			}
		});
	}

	@Override
	public void doMigrate() {
		SequenceCommandChain sequenceCommandChain = new SequenceCommandChain(true);

		sequenceCommandChain.add(prevPrimaryDcMigrateCmd(cluster, shard, prevPrimaryDc));
		sequenceCommandChain.add(newPrimaryDcMigrate(cluster, shard, newPrimaryDc));

		logger.info("[doMigrate][start]{},{}", cluster, shard);
		sequenceCommandChain.execute().addListener(migrateFuture -> {
			notifyObservers(new ShardObserverEvent(shardName(), ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC, ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
			logger.info("[doMigrate][end]{},{}", cluster, shard);
		});
	}
	
	@Override
	public void doMigrateOtherDc() {
		
		logger.info("[doMigrateOtherDc]{}-{}, {}->{}", cluster, shard, prevPrimaryDc, newPrimaryDc);

		if(shardMigrationResult.stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
			for(DcTbl dc : dcs.values()) {
				if(!(dc.getDcName().equals(newPrimaryDc))) {
					doOtherDcMigrate(cluster, shard, dc.getDcName(), newPrimaryDc);
				}
			}
		}
		
		if(shardMigrationResult.stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
			shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE, true, LogUtils.info("Success"));
			shardMigrationResult.setStatus(ShardMigrationResultStatus.SUCCESS);
		} else {
			shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE, false, LogUtils.error("Failed"));
		}

		notifyObservers(new ShardObserverEvent(shardName(), ShardMigrationStep.MIGRATE));
	}

	@Override
	public void doRollBack() throws ShardMigrationException{
		
		logger.info("[tryRollback]{}-{}, {}<-{}", cluster, shard, prevPrimaryDc, newPrimaryDc);
		for(DcTbl dc : dcs.values()) {
			if(!(dc.getDcName().equals(prevPrimaryDc))) {
				doOtherDcRollback(dc.getDcName(), prevPrimaryDc);
			}
		}

		try {
			doRollBackPrevPrimaryDc(cluster, shard, prevPrimaryDc).get();
		} catch (Exception e) {
			throw new ShardMigrationException(String.format("%s,%s doRollBackPrevPrimaryDc:%s", cluster, shard, prevPrimaryDc), e);
		}
	}
	
	private Command<MetaServerConsoleService.PreviousPrimaryDcMessage> prevPrimaryDcMigrateCmd(String cluster, String shard, String dc) {

		shardMigrationResult.stepRetry(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC);
		Command<MetaServerConsoleService.PreviousPrimaryDcMessage> cmd = commandBuilder.buildPrevPrimaryDcCommand(cluster, shard, dc);
		cmd.future().addListener(new CommandFutureListener<MetaServerConsoleService.PreviousPrimaryDcMessage>() {
			@Override
			public void operationComplete(CommandFuture<MetaServerConsoleService.PreviousPrimaryDcMessage> commandFuture) throws Exception {

				if (commandFuture.isSuccess()) {
					MetaServerConsoleService.PreviousPrimaryDcMessage previousPrimaryDcMessage = commandFuture.get();
					logger.info("[doPrevPrimaryDcMigrate][result]{},{},{},{}", cluster, shard, dc, previousPrimaryDcMessage);
					shardMigrationResult.setPreviousPrimaryDcMessage(previousPrimaryDcMessage);
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC, true,
							previousPrimaryDcMessage == null? LogUtils.info("Succeed, return message null"): previousPrimaryDcMessage.getMessage());
				} else {
					logger.error("[doPrevPrimaryDcMigrate][fail]{},{},{}", cluster, shard, dc, commandFuture.cause());
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC, true, LogUtils.error("Ignore:" + commandFuture.cause().getMessage()));
				}

				notifyObservers(new ShardObserverEvent(shardName(), ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
			}
		});
		return cmd;
	}

	private Command<MetaServerConsoleService.PrimaryDcChangeMessage> newPrimaryDcMigrate(String cluster, String shard, String newPrimaryDc) {
		Command<MetaServerConsoleService.PrimaryDcChangeMessage> cmd =
				commandBuilder.buildNewPrimaryDcCommand(cluster, shard, newPrimaryDc, shardMigrationResult::getPreviousPrimaryDcMessage);

		cmd.future().addListener(new CommandFutureListener<PrimaryDcChangeMessage>() {
			@Override
			public void operationComplete(CommandFuture<PrimaryDcChangeMessage> commandFuture) throws Exception {
				if (commandFuture.isSuccess()) {
					PrimaryDcChangeMessage res = commandFuture.get();

					logger.info("[doNewPrimaryDcMigrate]{},{},{},{}", cluster, shard, newPrimaryDc, res);
					if(PRIMARY_DC_CHANGE_RESULT.SUCCESS.equals(res.getErrorType())) {
						shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC, true, res.getErrorMessage());
						shardMigrationResult.setNewMaster(new HostPort(res.getNewMasterIp(), res.getNewMasterPort()));
					} else {
						logger.error("[doNewPrimaryDcMigrate][fail]{},{}", cluster, shard);
						shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC, false, res.getErrorMessage());
					}
				} else {
					logger.error("[doNewPrimaryDcMigrate][fail]{},{}", cluster, shard, commandFuture.cause());
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC, false, LogUtils.error(commandFuture.cause().getMessage()));
				}

				notifyObservers(new ShardObserverEvent(shardName(), ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
			}
		});
		return cmd;
	}
	
	private void doOtherDcRollback(String dc, String prevPrimaryDc) {

		 Command<PrimaryDcChangeMessage>  command = commandBuilder.buildOtherDcCommand(cluster, shard, prevPrimaryDc, dc);
		 if(command == null){
			 logger.warn("[doOtherDcRollback][fail, command null]{}", this);
			 return;
		 }
		 CommandFuture<PrimaryDcChangeMessage> migrateResult = command.execute();
		migrateResult.addListener(new CommandFutureListener<PrimaryDcChangeMessage>() {

			@Override
			public void operationComplete(CommandFuture<PrimaryDcChangeMessage> commandFuture) throws Exception {
				if(!commandFuture.isSuccess()){
					logger.error("[doOtherDcRollback]" + cluster + "," + shard, commandFuture.cause());
				}else{
					PrimaryDcChangeMessage primaryDcChangeMessage = commandFuture.get();
					logger.info("[doOtherDcRollback]{}, {}, {}", cluster, shard, primaryDcChangeMessage);
				}
			}
		});

	}

	private CommandFuture<PrimaryDcChangeMessage> doOtherDcMigrate(String cluster, String shard, String dc, String newPrimaryDc) {

		CommandFuture<PrimaryDcChangeMessage> migrateResult = commandBuilder.buildOtherDcCommand(cluster, shard, newPrimaryDc, dc).execute();
		migrateResult.addListener(new CommandFutureListener<PrimaryDcChangeMessage>() {
			@Override
			public void operationComplete(CommandFuture<PrimaryDcChangeMessage> commandFuture) throws Exception {
				if (commandFuture.isSuccess()) {
					PrimaryDcChangeMessage res = commandFuture.get();
					logger.info("[doOtherDcMigrate]{},{},{}->{}, {}", cluster, shard, dc, newPrimaryDc, res);
					if(PRIMARY_DC_CHANGE_RESULT.SUCCESS.equals(res.getErrorType())) {
						shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_OTHER_DC, true, res.getErrorMessage());
					} else {
						shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_OTHER_DC, false, res.getErrorMessage());
					}
				} else {
					logger.error("[doOtherDcMigrate][fail]{},{},{}->{}", cluster, shard, dc, newPrimaryDc, commandFuture.cause());
					shardMigrationResult.updateStepResult(ShardMigrationStep.MIGRATE_OTHER_DC, false, commandFuture.cause().getMessage());
				}

				notifyObservers(new ShardObserverEvent(shardName(), ShardMigrationStep.MIGRATE_OTHER_DC));
			}
		});
		return migrateResult;
	}

	private CommandFuture<MetaServerConsoleService.PreviousPrimaryDcMessage> doRollBackPrevPrimaryDc(String cluster, String shard, String dc) {
		CommandFuture<MetaServerConsoleService.PreviousPrimaryDcMessage> migrateResult = commandBuilder.buildRollBackCommand(cluster, shard, dc).execute();
		migrateResult.addListener(new CommandFutureListener<MetaServerConsoleService.PreviousPrimaryDcMessage>() {
			@Override
			public void operationComplete(CommandFuture<MetaServerConsoleService.PreviousPrimaryDcMessage> commandFuture) throws Exception {
				if (commandFuture.isSuccess()) {
					MetaServerConsoleService.PreviousPrimaryDcMessage primaryDcChangeMessage = commandFuture.get();
					logger.info("[doPrevPrimaryDcMigrate]{},{},{}, {}", cluster, shard, dc, primaryDcChangeMessage);
				} else {
					logger.error("[doPrevPrimaryDcMigrate][fail]{},{},{}", cluster, shard, dc, commandFuture.cause());
				}
				notifyObservers(new ShardObserverEvent(shardName(), "doRollBackPrevPrimaryDc"));
			}
		});
		return migrateResult;
	}

	@Override
	public String toString() {
		return String.format("[DefaultMigrationShard]%s:%s,%s->%s", cluster, shard, prevPrimaryDc, newPrimaryDc);
	}

	@Override
	public String shardName() {
		return currentShard.getShardName();
	}

	@Override
	public ShardMigrationStepResult stepResult(ShardMigrationStep step) {
		return shardMigrationResult.stepResult(step);
	}

	@Override
	public void retry(ShardMigrationStep step) {
		shardMigrationResult.stepRetry(step);
	}

	@VisibleForTesting
	public void setCommandBuilder(MigrationCommandBuilder builder) {
		this.commandBuilder = builder;
	}

	public static class ShardObserverEvent{

		private String shardName;
		private ShardMigrationStep[] step;
		private String desc;

		public ShardObserverEvent(String shardName, ShardMigrationStep ...step){
			this.shardName = shardName;
			this.step = step;
		}

		public ShardObserverEvent(String shardName, String desc){
			this.shardName = shardName;
			this.desc = desc;
		}

		@Override
		public String toString() {

			if(step != null){
				return String.format("%s, step:%s", shardName, StringUtil.join(",", step));
			}else{
				return String.format("%s, desc:%s", shardName, desc);
			}
		}
	}
}
