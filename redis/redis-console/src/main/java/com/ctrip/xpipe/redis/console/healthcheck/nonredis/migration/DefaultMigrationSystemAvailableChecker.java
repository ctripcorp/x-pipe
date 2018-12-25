package com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractSiteLeaderIntervalCheck;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.migration.CheckMigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.service.migration.impl.CHECK_MIGRATION_SYSTEM_STEP;
import com.ctrip.xpipe.redis.console.service.migration.impl.DefaultCheckMigrationCommandBuilder;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultMigrationSystemAvailableChecker extends AbstractSiteLeaderIntervalCheck implements MigrationSystemAvailableChecker {

    private static final long checkIntervalMill = Long.parseLong(System.getProperty("console.migrate.system.check.interval", "3000"));

    @Autowired
    private DcService dcService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private AlertManager alertManager;

    private CheckMigrationCommandBuilder builder;

    private AtomicReference<MigrationSystemAvailableChecker.MigrationSystemAvailability> result
            = new AtomicReference<>(MigrationSystemAvailableChecker.MigrationSystemAvailability.createAvailableResponse());

    @PostConstruct
    public void initBuilder() {
        builder = new DefaultCheckMigrationCommandBuilder(scheduled, dcService, clusterService,
                OuterClientService.DEFAULT, consoleConfig);
    }

    @Override
    protected void doCheck() {
        MigrationSystemAvailability availability = result.getAndSet(MigrationSystemAvailability.createAvailableResponse());
        if(!availability.isAvaiable()) {
            alertManager.alert("", "", new HostPort(), ALERT_TYPE.MIGRATION_SYSTEM_UNAVAILABLE, availability.getMessage());
        }
        SequenceCommandChain chain = new SequenceCommandChain(true);
        chain.add(checkDatabase());
        chain.add(checkOuterClient());
        chain.add(checkMetaServer());
        chain.execute();
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.MIGRATION_SYSTEM_UNAVAILABLE);
    }

    @Override
    protected long getIntervalMilli() {
        return checkIntervalMill;
    }

    @Override
    public MigrationSystemAvailableChecker.MigrationSystemAvailability getResult() {
        return result.get();
    }

    private Command<RetMessage> checkDatabase() {
        Command<RetMessage> command = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_DATA_BASE);
        checkCommandFuture("Database:", command);
        return command;
    }

    private Command<RetMessage> checkOuterClient() {
        Command<RetMessage> command = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_OUTER_CLIENT);
        checkCommandFuture("OuterClient:", command);
        return command;
    }

    private Command<RetMessage> checkMetaServer() {
        Command<RetMessage> command = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_METASERVER);
        checkCommandFuture("MetaServer:", command);
        return command;
    }

    private void checkCommandFuture(final String title, final Command<RetMessage> command) {
        command.future().addListener(new CommandFutureListener<RetMessage>() {
            @Override
            public void operationComplete(CommandFuture<RetMessage> commandFuture) {
                if(!commandFuture.isSuccess()) {
                    getResult().addErrorMessage(title, commandFuture.cause());
                } else {
                    RetMessage retMessage = commandFuture.getNow();
                    if(retMessage.getState() != RetMessage.SUCCESS_STATE) {
                        getResult().addErrorMessage(title, retMessage.getMessage());
                    }
                }
            }
        });
    }

    @VisibleForTesting
    protected CheckMigrationCommandBuilder getBuilder() {
        return builder;
    }

}
