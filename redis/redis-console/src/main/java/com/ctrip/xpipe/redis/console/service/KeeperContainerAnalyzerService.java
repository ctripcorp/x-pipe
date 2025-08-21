package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;

import java.util.List;

public interface KeeperContainerAnalyzerService {

    void initStandard(List<KeeperContainerUsedInfoModel> currentDcAllKeeperContainerUsedInfoModelMap);

}
