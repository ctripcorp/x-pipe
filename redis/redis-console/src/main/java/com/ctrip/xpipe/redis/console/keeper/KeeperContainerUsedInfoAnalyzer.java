package com.ctrip.xpipe.redis.console.keeper;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;

import java.util.List;
import java.util.Map;

public interface KeeperContainerUsedInfoAnalyzer {

    void updateKeeperContainerUsedInfo(int index, List<KeeperContainerUsedInfoModel> keeperContainerUsedInfoModels);

    List<MigrationKeeperContainerDetailModel> getAllDcReadyToMigrationKeeperContainers();

    List<MigrationKeeperContainerDetailModel> getCurrentDcReadyToMigrationKeeperContainers();

    List<KeeperContainerUsedInfoModel> getAllDcKeeperContainerUsedInfoModelsList();

    List<KeeperContainerUsedInfoModel> getCurrentDcKeeperContainerUsedInfoModelsList();

}
