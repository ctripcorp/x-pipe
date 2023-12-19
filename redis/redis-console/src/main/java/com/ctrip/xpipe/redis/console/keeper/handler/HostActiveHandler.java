package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;

public class HostActiveHandler extends AbstractHandler<KeeperContainerUsedInfoModel>{

    @Override
    public boolean doNextHandler(KeeperContainerUsedInfoModel usedInfoModel) {
        return usedInfoModel.isKeeperContainerActive();
    }
}
