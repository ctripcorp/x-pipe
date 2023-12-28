package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.impl.DefaultKeeperContainerUsedInfoAnalyzer.*;

import java.util.Map;

public class KeeperPairOverloadHandler extends AbstractHandler<Map.Entry<DcClusterShardActive, KeeperContainerUsedInfoModel.KeeperUsedInfo>>{

    private Map<IPPair, IPPairData> keeperPairUsedInfoMap;

    private KeeperContainerUsedInfoModel keeperContainer1;

    private KeeperContainerUsedInfoModel keeperContainer2;

    private ConsoleConfig config;

    public KeeperPairOverloadHandler(Map<IPPair, IPPairData> keeperPairUsedInfoMap, KeeperContainerUsedInfoModel keeperContainer1, KeeperContainerUsedInfoModel keeperContainer2, ConsoleConfig config) {
        this.keeperPairUsedInfoMap = keeperPairUsedInfoMap;
        this.keeperContainer1 = keeperContainer1;
        this.keeperContainer2 = keeperContainer2;
        this.config = config;
    }

    @Override
    protected boolean doNextHandler(Map.Entry<DcClusterShardActive, KeeperUsedInfo> keeperUsedInfoEntry) {
        IPPair pair = new IPPair(keeperUsedInfoEntry.getValue().getKeeperIP(), keeperContainer2.getKeeperIp());
        IPPairData longLongPair = keeperPairUsedInfoMap.get(pair);
        if (longLongPair == null) return true;
        double keeperPairOverLoadFactor = config.getKeeperPairOverLoadFactor();
        double flowStandard = Math.min(keeperContainer1.getInputFlowStandard(), keeperContainer2.getInputFlowStandard()) * keeperPairOverLoadFactor;
        double peerDataStandard = Math.min(keeperContainer1.getRedisUsedMemoryStandard(), keeperContainer2.getRedisUsedMemoryStandard()) * keeperPairOverLoadFactor;
        return longLongPair.getInputFlow() + keeperUsedInfoEntry.getValue().getInputFlow() < flowStandard &&
                longLongPair.getPeerData() + keeperUsedInfoEntry.getValue().getPeerData() < peerDataStandard;
    }
}
