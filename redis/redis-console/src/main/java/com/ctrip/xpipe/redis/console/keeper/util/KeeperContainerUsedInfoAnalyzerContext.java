package com.ctrip.xpipe.redis.console.keeper.util;

import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;

import java.util.List;
import java.util.Map;

public interface KeeperContainerUsedInfoAnalyzerContext {

    boolean initKeeperPairData(Map<String, KeeperContainerUsedInfoModel> usedInfoMap);

    String getBackUpKeeperIp(DcClusterShard activeKeeper);

    List<String> getAllPairsIP(String ip);

    IPPairData getIPPairData(String ip1, String ip2);

    Map<DcClusterShardKeeper, KeeperUsedInfo> getAllDetailInfo(String ip1, String ip2);

    void updateMigrateIpPair(String srcKeeperIp, String srcKeeperIpPair, String targetKeeperIp, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> migrateDcClusterShard);

    long getMaxActiveRedisUsedMemory(Map<String, KeeperContainerUsedInfoModel> usedInfoMap);

}
