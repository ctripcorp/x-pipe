package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;

public class KeeperContainerPairOverloadHandler extends AbstractHandler<Object>{

    private KeeperContainerUsedInfoModel pairA;

    private KeeperContainerUsedInfoModel pairB;

    private ConsoleConfig config;

    private IPPairData ipPairData;

    public KeeperContainerPairOverloadHandler(KeeperContainerUsedInfoModel pairA, KeeperContainerUsedInfoModel pairB, ConsoleConfig config, IPPairData ipPairData) {
        this.pairA = pairA;
        this.pairB = pairB;
        this.config = config;
        this.ipPairData = ipPairData;
    }

    @Override
    protected boolean doNextHandler(Object o) {
        double keeperPairOverLoadFactor = config.getKeeperPairOverLoadFactor();
        KeeperContainerOverloadStandardModel minStandardModel = new KeeperContainerOverloadStandardModel()
                .setFlowOverload((long) (Math.min(pairB.getInputFlowStandard(), pairA.getInputFlowStandard()) * keeperPairOverLoadFactor))
                .setPeerDataOverload((long) (Math.min(pairB.getRedisUsedMemoryStandard(), pairA.getRedisUsedMemoryStandard()) * keeperPairOverLoadFactor));
        long overloadInputFlow = ipPairData.getInputFlow() - minStandardModel.getFlowOverload();
        long overloadPeerData = ipPairData.getPeerData() - minStandardModel.getPeerDataOverload();
        return overloadInputFlow > 0 || overloadPeerData > 0;
    }
}
