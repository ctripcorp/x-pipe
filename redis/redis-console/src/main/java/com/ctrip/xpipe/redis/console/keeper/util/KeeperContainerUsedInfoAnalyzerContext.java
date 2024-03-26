package com.ctrip.xpipe.redis.console.keeper.util;

import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;

import java.util.List;
import java.util.Map;

public interface KeeperContainerUsedInfoAnalyzerContext {

    void initKeeperPairData(List<KeeperContainerUsedInfoModel> usedInfoMap, Map<String, KeeperContainerUsedInfoModel> srcModelsMap);

    void initAvailablePool(List<KeeperContainerUsedInfoModel> usedInfoMap);

    void recycleKeeperContainer(KeeperContainerUsedInfoModel keeperContainer, boolean isPeerDataOverload);

    KeeperContainerUsedInfoModel getBestKeeperContainer(KeeperContainerUsedInfoModel  usedInfoModel, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> dcClusterShard, KeeperContainerUsedInfoModel srcKeeperPair, boolean isPeerDataOverload);

    String getBackUpKeeperIp(DcClusterShard activeKeeper);

    List<String> getAllPairsIP(String ip);

    IPPairData getIPPairData(String ip1, String ip2);

    void updateMigrateIpPair(String srcKeeperIp, String srcKeeperIpPair, String targetKeeperIp, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> migrateDcClusterShard);

    void addMigrationPlan(KeeperContainerUsedInfoModel src, KeeperContainerUsedInfoModel target, boolean switchActive, boolean keeperPairOverload, String cause, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> dcClusterShard, KeeperContainerUsedInfoModel srcPair);

    List<MigrationKeeperContainerDetailModel> getAllMigrationPlans();

    void addResourceLackPlan(KeeperContainerUsedInfoModel src, String srcOverLoadKeeperPairIp, String cause);

    boolean isProblemKeeperContainer(String keeperContainerIp);

}
