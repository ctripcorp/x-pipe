package com.ctrip.xpipe.redis.console.service.migration;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public interface BeaconMigrationService {

    CommandFuture<?> migrate(BeaconMigrationRequest migrationRequest);

    CommandFuture<?> biMigrate(BeaconMigrationRequest migrationRequest);
}
