package com.ctrip.xpipe.redis.console.controller.api.migrate;

import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationResponse;
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

/**
 * @author lishanglin
 * date 2020/12/28
 */
@RestController
@RequestMapping("/api/beacon/migration")
public class MigrationApi4Beacon {

    @Autowired
    private BeaconMigrationService beaconMigrationService;

    Logger logger = LoggerFactory.getLogger(MigrationApi4Beacon.class);

    @PostMapping(value = "/sync")
    public DeferredResult<BeaconMigrationResponse> syncMigrate(@RequestBody BeaconMigrationRequest migrationRequest) {
        DeferredResult<BeaconMigrationResponse> response = new DeferredResult<>();

        try {
            long eventId = beaconMigrationService.buildMigration(migrationRequest);
            beaconMigrationService.doMigration(eventId, migrationRequest.getClusterId()).addListener(migrationFuture -> {
                if (migrationFuture.isSuccess()) {
                    if (migrationFuture.get()) response.setResult(BeaconMigrationResponse.success());
                    else response.setResult(BeaconMigrationResponse.fail("migration fail"));
                } else {
                    response.setResult(BeaconMigrationResponse.fail(migrationFuture.cause().getMessage()));
                }
            });
        } catch (MigrationSystemNotHealthyException | ClusterNotFoundException | WrongClusterMetaException | NoAvailableDcException | MigrationConflictException e) {
            logger.info("[syncMigrate][{}] fail and skip", migrationRequest.getClusterName(), e);
            response.setResult(BeaconMigrationResponse.skip(e.getMessage()));
        } catch (MigrationNoNeedException e) {
            logger.info("[syncMigrate][{}] no need and success", migrationRequest.getClusterName(), e);
            response.setResult(BeaconMigrationResponse.success());
        } catch (MigrationNotSupportException | UnknownTargetDcException e) {
            logger.warn("[syncMigrate][{}] unexpect migration", migrationRequest.getClusterName(), e);
            response.setResult(BeaconMigrationResponse.skip(e.getMessage()));
        } catch (Throwable th) {
            logger.info("[syncMigrate][{}] fail, unknown reason", migrationRequest.getClusterName(), th);
            response.setResult(BeaconMigrationResponse.fail(th.getMessage()));
        }

        return response;
    }

}
