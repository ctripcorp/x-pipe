package com.ctrip.xpipe.redis.console.keeper.handler;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;
import com.ctrip.xpipe.redis.console.keeper.util.KeeperContainerUsedInfoAnalyzerContext;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class KeeperContainerFilterChain {

    @Autowired
    private ConsoleConfig config;

    public boolean isKeeperContainerUseful(KeeperContainerUsedInfoModel targetContainer){
        Handler<KeeperContainerUsedInfoModel> handler = new HostActiveHandler();
        handler.setNextHandler(new HostDiskOverloadHandler(config))
                .setNextHandler(new KeeperContainerOverloadHandler());
        return handler.handle(targetContainer);
    }

    public boolean canMigrate(Map.Entry<DcClusterShardKeeper, KeeperContainerUsedInfoModel.KeeperUsedInfo> keeperUsedInfoEntry,
                              KeeperContainerUsedInfoModel srcKeeperContainer,
                              KeeperContainerUsedInfoModel targetKeeperContainer,
                              KeeperContainerUsedInfoAnalyzerContext analyzerUtil){
        Handler<Map.Entry<DcClusterShardKeeper, KeeperContainerUsedInfoModel.KeeperUsedInfo>> handler = new KeeperDataOverloadHandler(targetKeeperContainer);
        handler.setNextHandler(new KeeperPairOverloadHandler(analyzerUtil, srcKeeperContainer, targetKeeperContainer, config));
        return handler.handle(keeperUsedInfoEntry);
    }

    public boolean isKeeperPairOverload(Map.Entry<DcClusterShardKeeper, KeeperContainerUsedInfoModel.KeeperUsedInfo> keeperUsedInfoEntry,
                                        KeeperContainerUsedInfoModel keeperContainer1,
                                        KeeperContainerUsedInfoModel keeperContainer2,
                                        KeeperContainerUsedInfoAnalyzerContext analyzerUtil) {
        if (keeperContainer1.getKeeperIp().equals(keeperContainer2.getKeeperIp())) {
            return true;
        }
        return !new KeeperPairOverloadHandler(analyzerUtil, keeperContainer1, keeperContainer2, config)
                .handle(keeperUsedInfoEntry);
    }

    public boolean isKeeperContainerPairOverload(KeeperContainerUsedInfoModel keeperContainer1,
                                                 KeeperContainerUsedInfoModel keeperContainer2,
                                                 IPPairData ipPairData) {
        if (keeperContainer1.getKeeperIp().equals(keeperContainer2.getKeeperIp())) {
            return true;
        }
        return new KeeperContainerPairOverloadHandler(keeperContainer1, keeperContainer2, config, ipPairData).handle(null);
    }

    public boolean isDataOverLoad(KeeperContainerUsedInfoModel model) {
        return !new KeeperContainerOverloadHandler().handle(model);
    }

    @VisibleForTesting
    public void setConfig(ConsoleConfig config){
        this.config = config;
    }

}
