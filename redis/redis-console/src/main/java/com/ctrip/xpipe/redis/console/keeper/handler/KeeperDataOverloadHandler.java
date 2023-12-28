package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;

import java.util.Map;

public class KeeperDataOverloadHandler extends AbstractHandler<Map.Entry<DcClusterShardActive, KeeperUsedInfo>>{

    private KeeperContainerUsedInfoModel targetKeeperContainer;


    public KeeperDataOverloadHandler(KeeperContainerUsedInfoModel targetKeeperContainer) {
        this.targetKeeperContainer = targetKeeperContainer;
    }

    @Override
    protected boolean doNextHandler(Map.Entry<DcClusterShardActive, KeeperUsedInfo> keeperUsedInfoEntry) {
        return keeperUsedInfoEntry.getValue().getInputFlow() + targetKeeperContainer.getActiveInputFlow() < targetKeeperContainer.getInputFlowStandard() &&
                keeperUsedInfoEntry.getValue().getPeerData() + targetKeeperContainer.getActiveRedisUsedMemory() < targetKeeperContainer.getRedisUsedMemoryStandard();
    }
}
