package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;

import java.util.Map;

public class KeeperDataOverloadHandler extends AbstractHandler<Map.Entry<DcClusterShardKeeper, KeeperUsedInfo>>{

    private KeeperContainerUsedInfoModel targetKeeperContainer;


    public KeeperDataOverloadHandler(KeeperContainerUsedInfoModel targetKeeperContainer) {
        this.targetKeeperContainer = targetKeeperContainer;
    }

    @Override
    protected boolean doNextHandler(Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> keeperUsedInfoEntry) {
        return keeperUsedInfoEntry.getValue().getInputFlow() + targetKeeperContainer.getActiveInputFlow() < targetKeeperContainer.getInputFlowStandard() &&
                keeperUsedInfoEntry.getValue().getPeerData() + targetKeeperContainer.getActiveRedisUsedMemory() < targetKeeperContainer.getRedisUsedMemoryStandard();
    }
}
