package com.ctrip.xpipe.redis.console.keeper;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;

import java.util.List;
import java.util.Map;

public interface KeeperContainerUsedInfoAnalyzer {

    void updateKeeperContainerUsedInfo(String dcName, Map<String, KeeperContainerUsedInfoModel> modelMap);

    List<MigrationKeeperContainerDetailModel> getAllDcReadyToMigrationKeeperContainers();

    List<MigrationKeeperContainerDetailModel> getCurrentDcReadyToMigrationKeeperContainers();

    List<KeeperContainerUsedInfoModel> getAllDcKeeperContainerUsedInfoModelsList();

    List<KeeperContainerUsedInfoModel> getCurrentDcKeeperContainerUsedInfoModelsList();

}
