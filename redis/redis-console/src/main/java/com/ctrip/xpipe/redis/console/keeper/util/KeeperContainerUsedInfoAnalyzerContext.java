package com.ctrip.xpipe.redis.console.keeper.util;

import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.KeeperUsedInfo;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;

import java.util.List;
import java.util.Map;

public interface KeeperContainerUsedInfoAnalyzerContext {

    void initKeeperPairData(List<KeeperContainerUsedInfoModel> usedInfoMap, Map<Long, KeeperContainerUsedInfoModel> srcModelsMap);

    void initAvailablePool(List<KeeperContainerUsedInfoModel> usedInfoMap);

    void recycleKeeperContainer(KeeperContainerUsedInfoModel keeperContainer, boolean isPeerDataOverload);

    KeeperContainerUsedInfoModel getBestKeeperContainer(KeeperContainerUsedInfoModel  usedInfoModel, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> dcClusterShard, KeeperContainerUsedInfoModel srcKeeperPair, boolean isPeerDataOverload, boolean isMigrateShardBackUp);
    
    Long getBackUpKeeperContainerId(DcClusterShard activeKeeper);
    
    List<Long> getAllPairsContainerId(Long containerId);

    IPPairData getContainerPairData(Long containerId1, Long containerId2);

    void addMigrationPlan(KeeperContainerUsedInfoModel src, KeeperContainerUsedInfoModel target, boolean switchActive, boolean keeperPairOverload, String cause, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> dcClusterShard, KeeperContainerUsedInfoModel srcPair);

    List<MigrationKeeperContainerDetailModel> getAllMigrationPlans();

    void addResourceLackPlan(KeeperContainerUsedInfoModel src, String srcOverLoadKeeperPairIp, String cause);

    boolean isProblemKeeperContainerById(Long keeperContainerId);

}
