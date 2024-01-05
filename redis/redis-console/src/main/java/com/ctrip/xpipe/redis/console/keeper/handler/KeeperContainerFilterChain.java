package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.util.DefaultKeeperContainerUsedInfoAnalyzerUtil;
import com.ctrip.xpipe.redis.console.keeper.util.KeeperContainerUsedInfoAnalyzerUtil;
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
                                  KeeperContainerUsedInfoAnalyzerUtil analyzerUtil){
        return new KeeperDataOverloadHandler(targetKeeperContainer)
                .setNextHandler(new KeeperPairOverloadHandler(analyzerUtil, srcKeeperContainer, targetKeeperContainer, config))
                .handle(keeperUsedInfoEntry);
    }

    public boolean doKeeperPairFilter(Map.Entry<DcClusterShardActive, KeeperContainerUsedInfoModel.KeeperUsedInfo> keeperUsedInfoEntry,
                                      KeeperContainerUsedInfoModel keeperContainer1,
                                      KeeperContainerUsedInfoModel keeperContainer2,
                                      KeeperContainerUsedInfoAnalyzerUtil analyzerUtil) {
        return new KeeperPairOverloadHandler(analyzerUtil, keeperContainer1, keeperContainer2, config)
                .handle(keeperUsedInfoEntry);
    }

    @VisibleForTesting
    public void setConfig(ConsoleConfig config){
        this.config = config;
    }

}
