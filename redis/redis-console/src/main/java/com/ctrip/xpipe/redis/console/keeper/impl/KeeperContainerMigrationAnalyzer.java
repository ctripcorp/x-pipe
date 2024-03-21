package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;

import java.util.List;
import java.util.Map;

public interface KeeperContainerMigrationAnalyzer {

    List<MigrationKeeperContainerDetailModel> getMigrationPlans(Map<String, KeeperContainerUsedInfoModel> modelsMap);

}
