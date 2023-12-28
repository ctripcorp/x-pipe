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

    public boolean doKeeperContainerFilter(KeeperContainerUsedInfoModel targetContainer){
        return new HostActiveHandler()
                .setNextHandler(new HostDiskOverloadHandler(config))
                .setNextHandler(new KeeperContainerOverloadHandler())
                .handle(targetContainer);
    }

    public boolean doKeeperFilter(Map.Entry<DcClusterShardActive, KeeperContainerUsedInfoModel.KeeperUsedInfo> keeperUsedInfoEntry,
                                  KeeperContainerUsedInfoModel srcKeeperContainer,
                            KeeperContainerUsedInfoModel targetKeeperContainer,
                            Map<DefaultKeeperContainerUsedInfoAnalyzer.IPPair, DefaultKeeperContainerUsedInfoAnalyzer.IPPairData> keeperPairUsedInfoMap){
        return new KeeperDataOverloadHandler(targetKeeperContainer)
                .setNextHandler(new KeeperPairOverloadHandler(keeperPairUsedInfoMap, srcKeeperContainer, targetKeeperContainer, config))
                .handle(keeperUsedInfoEntry);
    }

    public boolean doKeeperPairFilter(Map.Entry<DcClusterShardActive, KeeperContainerUsedInfoModel.KeeperUsedInfo> keeperUsedInfoEntry,
                                      KeeperContainerUsedInfoModel keeperContainer1,
                                      KeeperContainerUsedInfoModel keeperContainer2,
                                      Map<DefaultKeeperContainerUsedInfoAnalyzer.IPPair, DefaultKeeperContainerUsedInfoAnalyzer.IPPairData> keeperPairUsedInfoMap) {
        return new KeeperPairOverloadHandler(keeperPairUsedInfoMap, keeperContainer1, keeperContainer2, config)
                .handle(keeperUsedInfoEntry);
    }

    @VisibleForTesting
    public void setConfig(ConsoleConfig config){
        this.config = config;
    }

}
