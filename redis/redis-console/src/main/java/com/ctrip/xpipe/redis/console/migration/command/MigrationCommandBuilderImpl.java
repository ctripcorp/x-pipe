package com.ctrip.xpipe.redis.console.migration.command;

import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public enum MigrationCommandBuilderImpl implements MigrationCommandBuilder {
	INSTANCE;

	private MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper = new DefaultMetaServerConsoleServiceManagerWrapper();
	
	public Command<PrimaryDcCheckMessage> buildDcCheckCommand(final String cluster, final String shard, final String dc, final String newPrimaryDc) {
		return new AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage>() {

			@Override
			public String getName() {
				return "PrimaryDcCheck";
			}

			@Override
			protected void doExecute() throws Exception {
				PrimaryDcCheckMessage result = null;
				try {
					result = metaServerConsoleServiceManagerWrapper
						.get(dc)
						.changePrimaryDcCheck(cluster, shard, newPrimaryDc);
					future().setSuccess(result);
				} catch (Exception e) {
					logger.error("[MigrateDcCheck][Failed]{}-{}-{}-{}", cluster, shard, dc, newPrimaryDc);
					if(dc.equals(newPrimaryDc)) {
						future().setSuccess(new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.FAIL,
								e.getMessage()));
					} else {
						future().setSuccess(new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.SUCCESS,
								String.format("Ingore on check failed with non-newPrimaryDc.%s-%s,%s,%s",
										cluster, shard, dc, newPrimaryDc)));
					}
				}
			}

			@Override
			protected void doReset() {
			}
		};
	}
	
	
	public Command<PrimaryDcChangeMessage> buildPrevPrimaryDcCommand(final String cluster, final String shard, final String prevPrimaryDc) {
		return new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

			@Override
			public String getName() {
				return "PrimaryDcChange-PrevPrimary";
			}

			@Override
			protected void doExecute() throws Exception {
				PrimaryDcChangeMessage result = null;
				try {
					metaServerConsoleServiceManagerWrapper
						.get(prevPrimaryDc)
						.makeMasterReadOnly(cluster, shard, true);
					result = new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.SUCCESS, 
							"Previous primary dc migrate success.");
				} catch (Exception e) {
					logger.error("[PrimaryDcChange][PrevPrimaryDc][Failed]{}-{}", cluster, shard, e);
					result = new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.SUCCESS, 
							String.format("Ignore previous primary dc migration fail.Reason:%s", e));
				}
				future().setSuccess(result);
			}

			@Override
			protected void doReset() {
			}
		};
	}

	public Command<PrimaryDcChangeMessage> buildNewPrimaryDcCommand(final String cluster, final String shard, final String newPrimaryDc) {
		return new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

			@Override
			public String getName() {
				return "PrimaryDcChange-NewPrimary";
			}

			@Override
			protected void doExecute() throws Exception {
				PrimaryDcChangeMessage result = null;
				try {
					result = metaServerConsoleServiceManagerWrapper
							.get(newPrimaryDc)
							.doChangePrimaryDc(cluster, shard, newPrimaryDc);
				} catch (Exception e) {
					logger.error("[PrimaryDcChange][NewPrimaryDc][Failed]{}-{}", cluster, shard);
					result = new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.FAIL,
							String.format("New primary dc migration failed.Reason:", e));
				}
				
				future().setSuccess(result);
			}

			@Override
			protected void doReset() {
			}
		};
	}
	
	public Command<PrimaryDcChangeMessage> buildOtherDcCommand(final String cluster, final String shard, final String newPrimaryDc, final String otherDc) {
		return new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

			@Override
			public String getName() {
				return "PrimaryDcChange-OtherDc";
			}

			@Override
			protected void doExecute() throws Exception {
				PrimaryDcChangeMessage result = null;
				try {
					result = metaServerConsoleServiceManagerWrapper
							.get(otherDc)
							.doChangePrimaryDc(cluster, shard, newPrimaryDc);
				} catch (Exception e) {
					logger.error("[PrimaryDcChange][OtherDc][Failed]{}-{}", cluster, shard);
					result = new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.SUCCESS,
							String.format("Ignore other dc migration failed.Reason:", e));
				}
				
				future().setSuccess(result);
			}

			@Override
			protected void doReset() {
			}
		};
	}
}
