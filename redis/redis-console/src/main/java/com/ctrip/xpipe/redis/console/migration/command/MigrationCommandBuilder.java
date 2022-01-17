package com.ctrip.xpipe.redis.console.migration.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;

import java.util.function.Supplier;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationCommandBuilder {
    Command<MetaServerConsoleService.PrimaryDcCheckMessage> buildDcCheckCommand(final String cluster, final String shard, final String dc, final String newPrimaryDc);
    Command<MetaServerConsoleService.PreviousPrimaryDcMessage> buildPrevPrimaryDcCommand(final String cluster, final String shard, final String prevPrimaryDc);
    Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildNewPrimaryDcCommand(final String cluster, final String shard, final String newPrimaryDc,
                                                                                      MetaServerConsoleService.PreviousPrimaryDcMessage previousPrimaryDcMessage);
    default Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildNewPrimaryDcCommand(final String cluster, final String shard, final String newPrimaryDc,
                                                                                      Supplier<MetaServerConsoleService.PreviousPrimaryDcMessage> previousPrimaryDcMessageSupplier) {
        return buildNewPrimaryDcCommand(cluster, shard, newPrimaryDc, previousPrimaryDcMessageSupplier.get());
    }
    Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildOtherDcCommand(final String cluster, final String shard, final String primaryDc, final String executeDc);
    Command<MetaServerConsoleService.PreviousPrimaryDcMessage> buildRollBackCommand(final String cluster, final String shard, final String prevPrimaryDc);
}
