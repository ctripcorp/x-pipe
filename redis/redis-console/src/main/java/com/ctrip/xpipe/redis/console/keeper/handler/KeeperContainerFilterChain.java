package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.impl.DefaultKeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class KeeperContainerFilterChain {

    @Autowired
    private ConsoleConfig config;

    public boolean doKeeperContainerFilter(KeeperContainerUsedInfoModel targetContainer, KeeperContainerOverloadStandardModel targetStandard){
        return new HostActiveHandler()
                .setNextHandler(new HostDiskOverloadHandler(config))
                .setNextHandler(new KeeperContainerOverloadHandler(targetStandard))
                .handle(targetContainer);
    }

    public boolean doKeeperFilter(Map.Entry<DcClusterShardActive, KeeperContainerUsedInfoModel.KeeperUsedInfo> keeperUsedInfoEntry,
                            KeeperContainerUsedInfoModel targetKeeperContainer,
                            KeeperContainerOverloadStandardModel srcStandard,
                            KeeperContainerOverloadStandardModel targetStandard,
                            Map<DefaultKeeperContainerUsedInfoAnalyzer.IPPair, DefaultKeeperContainerUsedInfoAnalyzer.IPPairData> keeperPairUsedInfoMap){
        return new KeeperDataOverloadHandler(targetKeeperContainer, targetStandard)
                .setNextHandler(new KeeperPairOverloadHandler(keeperPairUsedInfoMap, targetKeeperContainer, srcStandard, targetStandard, config))
                .handle(keeperUsedInfoEntry);
    }

    public boolean doKeeperPairFilter(Map.Entry<DcClusterShardActive, KeeperContainerUsedInfoModel.KeeperUsedInfo> keeperUsedInfoEntry,
                                      KeeperContainerUsedInfoModel targetKeeperContainer,
                                      KeeperContainerOverloadStandardModel srcStandard,
                                      KeeperContainerOverloadStandardModel targetStandard,
                                      Map<DefaultKeeperContainerUsedInfoAnalyzer.IPPair, DefaultKeeperContainerUsedInfoAnalyzer.IPPairData> keeperPairUsedInfoMap) {
        return new KeeperPairOverloadHandler(keeperPairUsedInfoMap, targetKeeperContainer, srcStandard, targetStandard, config)
                .handle(keeperUsedInfoEntry);
    }

    @VisibleForTesting
    public void setConfig(ConsoleConfig config){
        this.config = config;
    }

}
