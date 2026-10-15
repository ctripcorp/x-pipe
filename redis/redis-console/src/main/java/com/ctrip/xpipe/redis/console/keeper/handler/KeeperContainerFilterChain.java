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
import java.util.Objects;

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
                              KeeperContainerUsedInfoModel srcKeeperContainerPair,
                              KeeperContainerUsedInfoModel targetKeeperContainer,
                              KeeperContainerUsedInfoAnalyzerContext analyzerUtil){
        Handler<Map.Entry<DcClusterShardKeeper, KeeperContainerUsedInfoModel.KeeperUsedInfo>> handler = new KeeperDataOverloadHandler(targetKeeperContainer);
        handler.setNextHandler(new KeeperPairOverloadHandler(analyzerUtil, srcKeeperContainerPair, targetKeeperContainer, config));
        return handler.handle(keeperUsedInfoEntry);
    }

    public boolean isMigrateKeeperPairOverload(Map.Entry<DcClusterShardKeeper, KeeperContainerUsedInfoModel.KeeperUsedInfo> keeperUsedInfoEntry,
                                               KeeperContainerUsedInfoModel keeperContainer1,
                                               KeeperContainerUsedInfoModel keeperContainer2,
                                               KeeperContainerUsedInfoAnalyzerContext analyzerUtil) {
        if (isSameKeeperContainer(keeperContainer1, keeperContainer2)) {
            return true;
        }
        return !new KeeperPairOverloadHandler(analyzerUtil, keeperContainer1, keeperContainer2, config)
                .handle(keeperUsedInfoEntry);
    }

    public boolean isKeeperContainerPairOverload(KeeperContainerUsedInfoModel keeperContainer1,
                                                 KeeperContainerUsedInfoModel keeperContainer2,
                                                 IPPairData ipPairData) {
        if (isSameKeeperContainer(keeperContainer1, keeperContainer2)) {
            return true;
        }
        return new KeeperContainerPairOverloadHandler(keeperContainer1, keeperContainer2, config, ipPairData).handle(null);
    }

    public boolean isDataOverLoad(KeeperContainerUsedInfoModel model) {
        return !new KeeperContainerOverloadHandler().handle(model);
    }

    /**
     * 一对 keeper 落在同一个 keeperContainer 上就没有容灾意义，直接按超载处理。
     * <p>
     * 用 keeperContainerId 判定而不是 IP：容器形态下所有 pod 的 IP 都是同一个占位值，
     * 按 IP 比会把两个不同的 pod 误判成同一台。id 缺失时同样返回 true，
     * 宁可拒绝迁移也不要迁到一个无法定位的容器上。
     */
    private boolean isSameKeeperContainer(KeeperContainerUsedInfoModel keeperContainer1,
                                          KeeperContainerUsedInfoModel keeperContainer2) {
        return Objects.equals(keeperContainer1.getKeeperContainerId(), keeperContainer2.getKeeperContainerId());
    }

    @VisibleForTesting
    public void setConfig(ConsoleConfig config){
        this.config = config;
    }

}
