package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.impl.DefaultKeeperContainerUsedInfoAnalyzer.*;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.tuple.Pair;

import java.util.Map;

public class KeeperPairOverloadHandler extends AbstractHandler<Map.Entry<DcClusterShardActive, KeeperContainerUsedInfoModel.KeeperUsedInfo>>{

    private Map<IPPair, IPPairData> keeperPairUsedInfoMap;

    private KeeperContainerUsedInfoModel targetKeeperContainer;

    private KeeperContainerOverloadStandardModel srcStandard;

    private KeeperContainerOverloadStandardModel targetStandard;

    private ConsoleConfig config;

    public KeeperPairOverloadHandler(Map<IPPair, IPPairData> keeperPairUsedInfoMap, KeeperContainerUsedInfoModel targetKeeperContainer, KeeperContainerOverloadStandardModel srcStandard, KeeperContainerOverloadStandardModel targetStandard, ConsoleConfig config) {
        this.keeperPairUsedInfoMap = keeperPairUsedInfoMap;
        this.targetKeeperContainer = targetKeeperContainer;
        this.srcStandard = srcStandard;
        this.targetStandard = targetStandard;
        this.config = config;
    }

    @Override
    protected boolean doNextHandler(Map.Entry<DcClusterShardActive, KeeperUsedInfo> keeperUsedInfoEntry) {
        IPPair pair = new IPPair(keeperUsedInfoEntry.getValue().getKeeperIP(), targetKeeperContainer.getKeeperIp());
        IPPairData longLongPair = keeperPairUsedInfoMap.get(pair);
        if (longLongPair == null) return true;
        double keeperPairOverLoadFactor = config.getKeeperPairOverLoadFactor();
        double flowStandard = Math.min(srcStandard.getFlowOverload(), targetStandard.getFlowOverload()) * keeperPairOverLoadFactor;
        double peerDataStandard = Math.min(srcStandard.getPeerDataOverload(), targetStandard.getPeerDataOverload()) * keeperPairOverLoadFactor;
        return longLongPair.getInputFlow() + keeperUsedInfoEntry.getValue().getInputFlow() < flowStandard &&
                longLongPair.getPeerData() + keeperUsedInfoEntry.getValue().getPeerData() < peerDataStandard;
    }
}
