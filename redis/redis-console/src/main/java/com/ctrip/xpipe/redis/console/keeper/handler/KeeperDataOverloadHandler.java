package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;

import java.util.Map;

public class KeeperDataOverloadHandler extends AbstractHandler<Map.Entry<DcClusterShardActive, KeeperUsedInfo>>{

    private KeeperContainerUsedInfoModel targetKeeperContainer;

    private KeeperContainerOverloadStandardModel targetStandard;

    public KeeperDataOverloadHandler(KeeperContainerUsedInfoModel targetKeeperContainer, KeeperContainerOverloadStandardModel targetStandard) {
        this.targetKeeperContainer = targetKeeperContainer;
        this.targetStandard = targetStandard;
    }

    @Override
    protected boolean doNextHandler(Map.Entry<DcClusterShardActive, KeeperUsedInfo> keeperUsedInfoEntry) {
        return keeperUsedInfoEntry.getValue().getInputFlow() + targetKeeperContainer.getActiveInputFlow() < targetStandard.getFlowOverload() &&
                keeperUsedInfoEntry.getValue().getPeerData() + targetKeeperContainer.getActiveRedisUsedMemory() < targetStandard.getPeerDataOverload();
    }
}
