package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.CommandChainException;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.migration.MigrationResources;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.console.service.migration.BeaconMigrationService;
import com.ctrip.xpipe.redis.console.service.migration.cmd.beacon.*;
import com.ctrip.xpipe.redis.console.service.migration.exception.UnexpectMigrationDataException;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.*;

/**
 * @author lishanglin
 * date 2020/12/28
 */
@Service
public class BeaconMigrationServiceImpl implements BeaconMigrationService {

    private MigrationSystemAvailableChecker checker;

    private MigrationEventManager migrationEventManager;

    private ConfigService configService;

    private ClusterService clusterService;

    private DcClusterService dcClusterService;

    private MigrationEventDao migrationEventDao;

    private MigrationClusterDao migrationClusterDao;

    private BeaconMetaService beaconMetaService;

    private DcCache dcCache;

    private ConsoleConfig config;

    private AlertManager alertManager;

    @Resource( name = MigrationResources.MIGRATION_PREPARE_EXECUTOR )
    private Executor prepareExecutors;

    @Resource(name = MigrationResources.MIGRATION_EXECUTOR)
    private Executor migrationExecutors;

    private ScheduledExecutorService scheduled;

    private Logger logger = LoggerFactory.getLogger(BeaconMigrationServiceImpl.class);

    @Autowired
    public BeaconMigrationServiceImpl(MigrationSystemAvailableChecker checker, MigrationEventManager migrationEventManager,
                                      ConfigService configService, ConsoleConfig config, DcCache dcCache,
                                      ClusterService clusterService, DcClusterService dcClusterService,
                                      MigrationEventDao migrationEventDao, MigrationClusterDao migrationClusterDao,
                                      BeaconMetaService beaconMetaService, AlertManager alertManager) {
        this.checker = checker;
        this.migrationEventManager = migrationEventManager;
        this.configService = configService;
        this.config = config;
        this.dcCache = dcCache;
        this.clusterService = clusterService;
        this.dcClusterService = dcClusterService;
        this.migrationEventDao = migrationEventDao;
        this.migrationClusterDao = migrationClusterDao;
        this.beaconMetaService = beaconMetaService;
        this.alertManager = alertManager;
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("BeaconMigrationTimeout"));
    }

    @Override
    public CommandFuture<?> migrate(BeaconMigrationRequest migrationRequest) {
        logger.debug("[migrate][{}] begin", migrationRequest.getClusterName());
        SequenceCommandChain migrateSequenceCmd = new SequenceCommandChain();
        migrateSequenceCmd.add(new MigrationPreCheckCmd(migrationRequest, checker, configService, clusterService, dcCache, beaconMetaService));
        migrateSequenceCmd.add(new MigrationFetchProcessingEventCmd(migrationRequest, clusterService, migrationClusterDao, dcCache));
        migrateSequenceCmd.add(new MigrationChooseTargetDcCmd(migrationRequest, dcCache, dcClusterService));
        migrateSequenceCmd.add(new MigrationBuildEventCmd(migrationRequest, migrationEventDao, migrationEventManager));
        migrateSequenceCmd.add(new MigrationDoExecuteCmd(migrationRequest, migrationEventManager, migrationExecutors));
        CommandFuture<?> future = migrateSequenceCmd.execute(prepareExecutors);

        long timeoutMilli = config.getMigrationTimeoutMilli();
        ScheduledFuture<?> scheduledFuture = scheduled.schedule(() -> {
            if (future.isDone()) {
                // already done, do nothing
            } else if (migrateSequenceCmd.executeCount() <= 0) {
                logger.info("[migrate][{}] timeout", migrationRequest.getClusterName());
                future.cancel(false);
            } else {
                logger.info("[migrate][{}] timeout but already running, continue", migrationRequest.getClusterName());
            }
        }, timeoutMilli, TimeUnit.MILLISECONDS);

        future.addListener(commandFuture -> {
            boolean cancelTimeout = true;
            if (commandFuture.isSuccess()) {
                // do nothing
            } else if (commandFuture.isCancelled()) {
                // already timeout
                cancelTimeout = false;
            } else if (!(commandFuture.cause() instanceof CommandChainException)) {
                logger.info("[migrate][{}] unexpected exception", migrationRequest.getClusterName(), commandFuture.cause());
            } else if (commandFuture.cause().getCause() instanceof UnexpectMigrationDataException) {
                alertManager.alert(migrationRequest.getClusterName(), null, null,
                        ALERT_TYPE.MIGRATION_DATA_CONFLICT, commandFuture.cause().getMessage());
            }

            if (cancelTimeout) {
                scheduledFuture.cancel(false);
            }
        });

        return future;
    }

}
