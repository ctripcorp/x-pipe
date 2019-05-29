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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicReference;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultMigrationSystemAvailableChecker extends AbstractSiteLeaderIntervalCheck implements MigrationSystemAvailableChecker {

    private static final long checkIntervalMill = Long.parseLong(System.getProperty("console.migrate.system.check.interval", "30000"));

    @Autowired
    private DcService dcService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private AlertManager alertManager;

    private static final long INIT_TIMESTAMP = -1L;

    private AtomicLong lastTimeCheckDatabase = new AtomicLong(INIT_TIMESTAMP);

    private AtomicLong lastTimeCheckOuterClient = new AtomicLong(INIT_TIMESTAMP);

    private AtomicLong lastTimeCheckMetaServer = new AtomicLong(INIT_TIMESTAMP);

    private volatile CheckMigrationCommandBuilder builder;

    private AtomicReference<MigrationSystemAvailableChecker.MigrationSystemAvailability> result
            = new AtomicReference<>(MigrationSystemAvailableChecker.MigrationSystemAvailability.createAvailableResponse());

    @Override
    protected void doCheck() {
        if(builder == null) {
            synchronized (this) {
                if(builder == null) {
                    builder = new DefaultCheckMigrationCommandBuilder(scheduled,
                            dcService,
                            clusterService,
                            OuterClientService.DEFAULT,
                            consoleConfig);
                }
            }
        }
        result.getAndSet(MigrationSystemAvailability.createAvailableResponse());

        SequenceCommandChain chain = new SequenceCommandChain(true);
        chain.add(checkDatabase());
        chain.add(checkOuterClient());
        chain.add(checkMetaServer());
        chain.execute().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                checkIfCheckCommandDelay();
            }
        });
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.MIGRATION_SYSTEM_CHECK_OVER_DUE);
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
        checkCommandFuture("Database:", command, lastTimeCheckDatabase);
        return command;
    }

    private Command<RetMessage> checkOuterClient() {
        Command<RetMessage> command = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_OUTER_CLIENT);
        checkCommandFuture("OuterClient:", command, lastTimeCheckOuterClient);
        return command;
    }

    private Command<RetMessage> checkMetaServer() {
        Command<RetMessage> command = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_METASERVER);
        checkCommandFuture("MetaServer:", command, lastTimeCheckMetaServer);
        return command;
    }

    private void checkCommandFuture(final String title, final Command<RetMessage> command,
                                    AtomicLong timestamp) {
        command.future().addListener(new CommandFutureListener<RetMessage>() {
            @Override
            public void operationComplete(CommandFuture<RetMessage> commandFuture) {
                timestamp.set(System.currentTimeMillis());
                if(!commandFuture.isSuccess()) {
                    warnOrError(title, commandFuture.cause());
                } else {
                    RetMessage retMessage = commandFuture.getNow();
                    if(retMessage.getState() != RetMessage.SUCCESS_STATE) {
                        warnOrError(title, retMessage);
                    }
                }
            }
        });
    }

    private void warnOrError(final String title, final RetMessage message) {
        if(message.getState() == RetMessage.FAIL_STATE) {
            getResult().addErrorMessage(title, message.getMessage());
        } else {
            getResult().addWarningMessage(title + message.getMessage());
        }
    }

    private void warnOrError(final String title, final Throwable throwable) {
        getResult().addErrorMessage(title, throwable);
    }

    private void checkIfCheckCommandDelay() {
        StringBuilder sb = new StringBuilder();
        checkUpdateTime(this.lastTimeCheckDatabase, sb, "Database");
        checkUpdateTime(this.lastTimeCheckOuterClient, sb, "OuterClient");
        checkUpdateTime(this.lastTimeCheckMetaServer, sb, "MetaServer");
        if(sb.length() > 1) {
            getResult().addWarningMessage(sb.toString());
            alertManager.alert("", "", new HostPort(), ALERT_TYPE.MIGRATION_SYSTEM_CHECK_OVER_DUE, sb.toString());
        }
    }

    private void checkUpdateTime(AtomicLong timestamp, StringBuilder sb, String title) {
        long threshold = TimeUnit.MINUTES.toMillis(1);
        if(timestamp.get() != INIT_TIMESTAMP && duration(timestamp.get()) > threshold) {
            sb.append(title).append(" not checked over 1 min\n");
        }
    }

    private long duration(long timestamp) {
        return System.currentTimeMillis() - timestamp;
    }

    @VisibleForTesting
    protected CheckMigrationCommandBuilder getBuilder() {
        return builder;
    }
}
