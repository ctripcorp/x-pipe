package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;

import java.util.Map;

public interface KeeperContainerAnalyzerService {

    void initStandard(Map<String, KeeperContainerUsedInfoModel> currentDcAllKeeperContainerUsedInfoModelMap);

}
