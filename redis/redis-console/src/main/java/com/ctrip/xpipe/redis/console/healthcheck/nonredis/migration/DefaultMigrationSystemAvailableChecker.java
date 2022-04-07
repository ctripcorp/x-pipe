package com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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

        MigrationSystemAvailability systemAvailability = MigrationSystemAvailability.createAvailableResponse();

        SequenceCommandChain chain = new SequenceCommandChain(true);
        chain.add(checkDatabase(systemAvailability));
        chain.add(checkOuterClient(systemAvailability));
        chain.add(checkMetaServer(systemAvailability));
        chain.execute().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                updateCheckResult(systemAvailability);
                checkIfCheckCommandDelay();
            }
        });
    }

    private void updateCheckResult(MigrationSystemAvailability newResult) {
        synchronized (result) {
            if (null == result.get() || result.get().getTimestamp() < newResult.getTimestamp()) {
                result.set(newResult);
            }
        }
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

    private Command<CheckResult> checkDatabase(MigrationSystemAvailability systemAvailability) {
        Command<CheckResult> command = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_DATA_BASE);
        checkCommandFuture("Database", command, systemAvailability, lastTimeCheckDatabase);
        return command;
    }

    private Command<CheckResult> checkOuterClient(MigrationSystemAvailability systemAvailability) {
        Command<CheckResult> command = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_OUTER_CLIENT);
        checkCommandFuture("OuterClient", command, systemAvailability, lastTimeCheckOuterClient);
        return command;
    }

    private Command<CheckResult> checkMetaServer(MigrationSystemAvailability systemAvailability) {
        Command<CheckResult> command = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_METASERVER);
        checkCommandFuture("MetaServer", command, systemAvailability, lastTimeCheckMetaServer);
        return command;
    }

    private void checkCommandFuture(final String title, final Command<CheckResult> command,
                                    MigrationSystemAvailability systemAvailability, AtomicLong timestamp) {
        command.future().addListener(new CommandFutureListener<CheckResult>() {
            @Override
            public void operationComplete(CommandFuture<CheckResult> commandFuture) {
                timestamp.set(System.currentTimeMillis());
                if(!commandFuture.isSuccess()) {
                    warnOrError(title, commandFuture.cause(), systemAvailability);
                } else {
                    CheckResult checkResult = commandFuture.getNow();
                    systemAvailability.addCheckResult(title, checkResult);
                    if(checkResult.getState() != RetMessage.SUCCESS_STATE) {
                        warnOrError(title, checkResult, systemAvailability);
                    }
                }
            }
        });
    }

    private void warnOrError(final String title, final RetMessage message, MigrationSystemAvailability systemAvailability) {
        if(message.getState() == RetMessage.FAIL_STATE) {
            systemAvailability.addErrorMessage(title, message.getMessage());
        } else {
            systemAvailability.addWarningMessage(String.format("%s:%s", title, message.getMessage()));
        }
    }

    private void warnOrError(final String title, final Throwable throwable, MigrationSystemAvailability systemAvailability) {
        systemAvailability.addErrorMessage(title, throwable);
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

    @Override
    protected boolean shouldCheck() {
        if (!super.shouldCheck()) return false;

        // only check migration system when the cluster in manage can migrate
        Set<String> ownTypes =  consoleConfig.getOwnClusterType();
        return null != ownTypes && ownTypes.stream().anyMatch(type -> ClusterType.lookup(type).supportMigration());
    }
}
