package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;

public class KeeperContainerOverloadHandler extends AbstractHandler<KeeperContainerUsedInfoModel>{

    @Override
    protected boolean doNextHandler(KeeperContainerUsedInfoModel usedInfoModel) {
        return usedInfoModel.getActiveInputFlow() < usedInfoModel.getInputFlowStandard() && usedInfoModel.getActiveRedisUsedMemory() < usedInfoModel.getRedisUsedMemoryStandard();
    }
}
