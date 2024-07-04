package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;

public class HostDiskOverloadHandler extends AbstractHandler<KeeperContainerUsedInfoModel>{

    private ConsoleConfig config;

    public HostDiskOverloadHandler(ConsoleConfig config) {
        this.config = config;
    }

    @Override
    public boolean doNextHandler(KeeperContainerUsedInfoModel usedInfoModel) {
        if (usedInfoModel.getDiskSize() == 0) {
            return false;
        }
        return (double) usedInfoModel.getDiskUsed() / usedInfoModel.getDiskSize() < config.getKeeperContainerDiskOverLoadFactor();
    }
}
