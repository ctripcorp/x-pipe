package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;

public class KeeperContainerOverloadHandler extends AbstractHandler<KeeperContainerUsedInfoModel>{

    KeeperContainerOverloadStandardModel targetStandard;

    public KeeperContainerOverloadHandler(KeeperContainerOverloadStandardModel targetStandard) {
        this.targetStandard = targetStandard;
    }

    @Override
    protected boolean doNextHandler(KeeperContainerUsedInfoModel usedInfoModel) {
        return usedInfoModel.getActiveInputFlow() < targetStandard.getFlowOverload() && usedInfoModel.getActiveRedisUsedMemory() < targetStandard.getPeerDataOverload();
    }
}
