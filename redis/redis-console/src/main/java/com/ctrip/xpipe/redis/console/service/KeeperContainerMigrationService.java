package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;

import java.util.List;

public interface KeeperContainerMigrationService {
    void beginMigrateKeeperContainers(List<MigrationKeeperContainerDetailModel> keeperContainerDetailModels);

    void stopMigrateKeeperContainers();

    List<MigrationKeeperContainerDetailModel> getMigrationProcess();
}
