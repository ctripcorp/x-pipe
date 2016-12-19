package com.ctrip.xpipe.redis.console.migration.command;

import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
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
					future().setFailure(e);
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
					
					future().setSuccess(result);
				} catch (Exception e) {
					logger.error("[PrimaryDcChange][PrevPrimaryDc][Failed]{}-{}", cluster, shard, e);
					future().setFailure(e);
				}
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

					future().setSuccess(result);
				} catch (Exception e) {
					logger.error("[PrimaryDcChange][NewPrimaryDc][Failed]{}-{}", cluster, shard);
					future().setFailure(e);
				}
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

					future().setSuccess(result);
				} catch (Exception e) {
					logger.error("[PrimaryDcChange][OtherDc][Failed]{}-{}", cluster, shard);
					future().setFailure(e);
				}
			}

			@Override
			protected void doReset() {
			}
		};
	}
}
