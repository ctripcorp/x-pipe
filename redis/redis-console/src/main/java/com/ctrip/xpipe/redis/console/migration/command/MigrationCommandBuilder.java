package com.ctrip.xpipe.redis.console.migration.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationCommandBuilder {
    Command<MetaServerConsoleService.PrimaryDcCheckMessage> buildDcCheckCommand(final String cluster, final String shard, final String dc, final String newPrimaryDc);
    Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildPrevPrimaryDcCommand(final String cluster, final String shard, final String prevPrimaryDc);
    Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildNewPrimaryDcCommand(final String cluster, final String shard, final String newPrimaryDc);
    Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildOtherDcCommand(final String cluster, final String shard, final String newPrimaryDc, final String otherDc);
}
