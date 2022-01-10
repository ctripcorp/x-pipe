package com.ctrip.xpipe.redis.console.migration.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.migration.MigrationResources;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.ReactorMetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.ReactorMetaServerConsoleServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultReactorMetaServerConsoleServiceManager;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

import java.util.function.Supplier;

/**
 * @author lishanglin
 * date 2021/9/24
 */
@Service
public class ReactorMigrationCommandBuilderImpl implements MigrationCommandBuilder {

    private ReactorMetaServerConsoleServiceManager metaServerConsoleServiceManager;

    private ConsoleConfig config;

    @Autowired
    public ReactorMigrationCommandBuilderImpl(@Qualifier(MigrationResources.MIGRATION_HTTP_LOOP_RESOURCE) LoopResources loopResources,
                                              @Qualifier(MigrationResources.MIGRATION_HTTP_CONNECTION_PROVIDER) ConnectionProvider connectionProvider,
                                              ConsoleConfig config) {
        this.metaServerConsoleServiceManager = new DefaultReactorMetaServerConsoleServiceManager(loopResources, connectionProvider);
        this.config = config;
    }

    @Override
    public Command<MetaServerConsoleService.PrimaryDcCheckMessage> buildDcCheckCommand(String cluster, String shard, String dc, String newPrimaryDc) {
        return new ReactorMigrationCmdWrap<>("ReactorDcCheckCommand", () ->
                getMetaServerConsoleService(dc).changePrimaryDcCheck(cluster, shard, newPrimaryDc)
        );
    }

    @Override
    public Command<MetaServerConsoleService.PreviousPrimaryDcMessage> buildPrevPrimaryDcCommand(String cluster, String shard, String prevPrimaryDc) {
        return new ReactorMigrationCmdWrap<>("ReactorPrevPrimaryDcCommand", () ->
                getMetaServerConsoleService(prevPrimaryDc).makeMasterReadOnly(cluster, shard, true)
        );
    }

    @Override
    public Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildNewPrimaryDcCommand(String cluster, String shard,
                                                                                             String newPrimaryDc,
                                                                                             MetaServerConsoleService.PreviousPrimaryDcMessage previousPrimaryDcMessage) {
        return new ReactorMigrationCmdWrap<>("ReactorNewPrimaryDcCommand", () ->
                getMetaServerConsoleService(newPrimaryDc).doChangePrimaryDc(cluster, shard, newPrimaryDc,
                        new MetaServerConsoleService.PrimaryDcChangeRequest(previousPrimaryDcMessage))
        );
    }

    @Override
    public Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildOtherDcCommand(String cluster, String shard, String primaryDc, String executeDc) {
        return new ReactorMigrationCmdWrap<>("ReactorOtherDcCommand", () ->
                getMetaServerConsoleService(executeDc).doChangePrimaryDc(cluster, shard, primaryDc, null)
        );
    }

    @Override
    public Command<MetaServerConsoleService.PreviousPrimaryDcMessage> buildRollBackCommand(String cluster, String shard, String prevPrimaryDc) {
        return new ReactorMigrationCmdWrap<>("ReactorRollBackCommand", () ->
                getMetaServerConsoleService(prevPrimaryDc).makeMasterReadOnly(cluster, shard, false)
        );
    }

    private ReactorMetaServerConsoleService getMetaServerConsoleService(String dc) {
        String metaAddress = config.getMetaservers().get(dc);
        if (StringUtil.isEmpty(metaAddress)) metaAddress = XPipeConsoleConstant.DEFAULT_ADDRESS;
        return metaServerConsoleServiceManager.getOrCreate(metaAddress);
    }

    private static class ReactorMigrationCmdWrap<T> extends AbstractCommand<T> {

        private String name;

        private Supplier<CommandFuture<T>> innerCmd;

        public ReactorMigrationCmdWrap(String name, Supplier<CommandFuture<T>> innerCmd) {
            this.name = name;
            this.innerCmd = innerCmd;
        }

        @Override
        protected void doExecute() throws Throwable {
            innerCmd.get().addListener(cmdFuture -> {
                if (cmdFuture.isSuccess()) future().setSuccess(cmdFuture.get());
                else future().setFailure(cmdFuture.cause());
            });
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return name;
        }
    }

}
