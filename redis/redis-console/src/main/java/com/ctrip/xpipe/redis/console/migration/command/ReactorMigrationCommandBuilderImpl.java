package com.ctrip.xpipe.redis.console.migration.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.migration.MigrationResources;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaserverAddress;
import com.ctrip.xpipe.redis.core.metaserver.ReactorMetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.ReactorMetaServerConsoleServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultReactorMetaServerConsoleServiceManager;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(ReactorMigrationCommandBuilderImpl.class);

    private ReactorMetaServerConsoleServiceManager metaServerConsoleServiceManager;

    private ConsoleConfig config;

    private MetaCache metaCache;

    @Autowired
    public ReactorMigrationCommandBuilderImpl(@Qualifier(MigrationResources.MIGRATION_HTTP_LOOP_RESOURCE) LoopResources loopResources,
                                              @Qualifier(MigrationResources.MIGRATION_HTTP_CONNECTION_PROVIDER) ConnectionProvider connectionProvider,
                                              ConsoleConfig config,
                                              MetaCache metaCache) {
        this.metaServerConsoleServiceManager = new DefaultReactorMetaServerConsoleServiceManager(loopResources, connectionProvider);
        this.config = config;
        this.metaCache = metaCache;
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
                        new MetaServerConsoleService.PrimaryDcChangeRequest(previousPrimaryDcMessage, addSentinelRequestValue(newPrimaryDc, cluster)))
        );
    }

    @Override
    public Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildNewPrimaryDcCommand(String cluster, String shard,
                                                                                             String newPrimaryDc,
                                                                                             Supplier<MetaServerConsoleService.PreviousPrimaryDcMessage> previousPrimaryDcMessageSupplier) {
        return new ReactorMigrationCmdWrap<>("ReactorNewPrimaryDcCommand", () ->
                getMetaServerConsoleService(newPrimaryDc).doChangePrimaryDc(cluster, shard, newPrimaryDc,
                        new MetaServerConsoleService.PrimaryDcChangeRequest(previousPrimaryDcMessageSupplier.get(), addSentinelRequestValue(newPrimaryDc, cluster)))
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
        return metaServerConsoleServiceManager.getOrCreate(new MetaserverAddress(dc, metaAddress));
    }

    private Boolean addSentinelRequestValue(String dcName, String clusterName) {
        try {
            ClusterMeta clusterMeta = getClusterMeta(dcName, clusterName);
            if (clusterMeta != null && clusterMeta.getOrgId() != null) {
                return config.supportSentinelBeacon(clusterMeta.getOrgId(), clusterName) ? Boolean.FALSE : Boolean.TRUE;
            }
        } catch (Exception e) {
            logger.warn("[addSentinelRequestValue][find cluster org fail]{}, {}", dcName, clusterName, e);
        }

        return config.supportSentinelBeacon(clusterName) ? Boolean.FALSE : Boolean.TRUE;
    }

    private ClusterMeta getClusterMeta(String dcName, String clusterName) {
        if (metaCache == null || metaCache.getXpipeMeta() == null) {
            return null;
        }

        DcMeta dcMeta = metaCache.getXpipeMeta().findDc(dcName);
        if (dcMeta == null) {
            return null;
        }

        return dcMeta.findCluster(clusterName);
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
