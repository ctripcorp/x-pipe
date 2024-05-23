package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;

import java.util.List;

public interface KeeperContainerMigrationService {
    boolean beginMigrateKeeperContainers(List<MigrationKeeperContainerDetailModel> keeperContainerDetailModels) throws Throwable;

    List<MigrationKeeperContainerDetailModel> getMigrationProcess();

    boolean stopMigrate();
}
