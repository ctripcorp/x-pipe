package com.ctrip.xpipe.redis.console.service.migration;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public interface BeaconMigrationService {

    long buildMigration(BeaconMigrationRequest migrationRequest) throws ClusterNotFoundException, WrongClusterMetaException,
            NoAvailableDcException, MigrationNotSupportException, MigrationSystemNotHealthyException,
            MigrationNoNeedException, UnknownTargetDcException, MigrationConflictException;

    CommandFuture<Boolean> doMigration(long eventId, long clusterId) throws Exception;

}
