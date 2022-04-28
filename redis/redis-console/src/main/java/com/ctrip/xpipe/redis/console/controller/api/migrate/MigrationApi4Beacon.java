package com.ctrip.xpipe.redis.console.controller.api.migrate;

import com.ctrip.xpipe.command.CommandChainException;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationResponse;
import com.ctrip.xpipe.redis.console.migration.exception.MigrationUnderProcessingException;
import com.ctrip.xpipe.redis.console.service.migration.BeaconMigrationService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import java.util.concurrent.RejectedExecutionException;

/**
 * @author lishanglin
 * date 2020/12/28
 */
@RestController
@RequestMapping("/api/beacon/migration")
public class MigrationApi4Beacon {

    @Autowired
    private BeaconMigrationService beaconMigrationService;

    @Autowired
    private ConsoleConfig config;

    Logger logger = LoggerFactory.getLogger(MigrationApi4Beacon.class);

    @PostMapping(value = "/sync")
    public DeferredResult<BeaconMigrationResponse> syncMigrate(@RequestBody BeaconMigrationRequest migrationRequest) {
        DeferredResult<BeaconMigrationResponse> response = new DeferredResult<>(config.getMigrationTimeoutMilli());

        try {
            beaconMigrationService.migrate(migrationRequest).addListener((commandFuture) -> {
                if (commandFuture.isSuccess()) {
                    response.setResult(BeaconMigrationResponse.success());
                    return;
                } else if (commandFuture.isCancelled()) {
                    response.setResult(BeaconMigrationResponse.fail("timeout"));
                    return;
                }

                Throwable cause = commandFuture.cause();
                if (cause instanceof CommandChainException) {
                    cause = cause.getCause();
                }

                if (cause instanceof MigrationSystemNotHealthyException || cause instanceof ClusterNotFoundException
                        || cause instanceof WrongClusterMetaException || cause instanceof NoAvailableDcException
                        || cause instanceof MigrationConflictException || cause instanceof MigrationJustFinishException
                        || cause instanceof AutoMigrationNotAllowException) {
                    logger.info("[syncMigrate][{}] fail and skip", migrationRequest.getClusterName(), cause);
                    response.setResult(BeaconMigrationResponse.skip(cause.getMessage()));
                } else if (cause instanceof MigrationNoNeedException) {
                    logger.info("[syncMigrate][{}] no need and success", migrationRequest.getClusterName(), cause);
                    response.setResult(BeaconMigrationResponse.success());
                } else if (cause instanceof MigrationUnderProcessingException) {
                    logger.info("[syncMigrate][{}] migration on processing, skip", migrationRequest.getClusterName(), cause);
                    response.setResult(BeaconMigrationResponse.skip(cause.getMessage()));
                } else if (cause instanceof MigrationNotSupportException || cause instanceof UnknownTargetDcException
                        || cause instanceof  MigrationCrossZoneException) {
                    logger.warn("[syncMigrate][{}] unexpect migration", migrationRequest.getClusterName(), cause);
                    response.setResult(BeaconMigrationResponse.skip(cause.getMessage()));
                } else {
                    logger.info("[syncMigrate][{}] fail", migrationRequest.getClusterName(), cause);
                    response.setResult(BeaconMigrationResponse.fail(cause.getMessage()));
                }
            });
        } catch (RejectedExecutionException e) {
            logger.info("[syncMigrate] reject, skip this round", e);
            response.setResult(BeaconMigrationResponse.skip(e.getMessage()));
        } catch (Throwable th) {
            logger.info("[syncMigrate] execute fail", th);
            response.setResult(BeaconMigrationResponse.fail(th.getMessage()));
        }

        return response;
    }

    @PostMapping(value = "/bi/sync")
    public DeferredResult<BeaconMigrationResponse> biSyncMigrate(@RequestBody BeaconMigrationRequest migrationRequest) {
        DeferredResult<BeaconMigrationResponse> response = new DeferredResult<>(config.getMigrationTimeoutMilli());

        try {
            beaconMigrationService.biMigrate(migrationRequest).addListener((commandFuture) -> {
                if (commandFuture.isSuccess()) {
                    response.setResult(BeaconMigrationResponse.success());
                    return;
                } else if (commandFuture.isCancelled()) {
                    response.setResult(BeaconMigrationResponse.fail("timeout"));
                    return;
                }

                Throwable cause = commandFuture.cause();
                if (cause instanceof CommandChainException) {
                    cause = cause.getCause();
                }

                response.setResult(BeaconMigrationResponse.fail(cause.getMessage()));
            });
        } catch (RejectedExecutionException e) {
            logger.info("[biSyncMigrate] reject, skip this round", e);
            response.setResult(BeaconMigrationResponse.skip(e.getMessage()));
        } catch (Throwable th) {
            logger.info("[biSyncMigrate] execute fail", th);
            response.setResult(BeaconMigrationResponse.fail(th.getMessage()));
        }

        return response;
    }

}
