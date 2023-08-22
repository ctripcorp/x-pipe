package com.ctrip.xpipe.redis.console.keeper;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;

import java.util.List;

public interface KeeperContainerUsedInfoAnalyzer {

    void updateKeeperContainerUsedInfo(int index, List<KeeperContainerInfoModel> keeperContainerInfoModels);

    List<MigrationKeeperContainerDetailModel> getAllDcReadyToMigrationKeeperContainers();
}
