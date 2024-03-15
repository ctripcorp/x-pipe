package com.ctrip.xpipe.redis.console.keeper.util;

import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class DefaultKeeperContainerUsedInfoAnalyzerContext implements KeeperContainerUsedInfoAnalyzerContext {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerUsedInfoAnalyzerContext.class);
    private Map<String, Map<String , IPPairData>> ipPairMap = new HashMap<>();
    private Map<DcClusterShardKeeper, KeeperUsedInfo> allDetailInfo = new HashMap<>();
    private static final String KEEPER_NO_BACKUP = "keeper_no_backup";

    public DefaultKeeperContainerUsedInfoAnalyzerContext() {}

    @Override
    public long getMaxActiveRedisUsedMemory(Map<String, KeeperContainerUsedInfoModel> usedInfo) {
        long max = 0;
        for (KeeperContainerUsedInfoModel usedInfoModel : usedInfo.values()) {
            max = Math.max(max, usedInfoModel.getActiveRedisUsedMemory());
        }
        return max;
    }

    @Override
    public boolean initKeeperPairData(Map<String, KeeperContainerUsedInfoModel> usedInfoMap) {
        ipPairMap.clear();
        allDetailInfo.clear();
        for (KeeperContainerUsedInfoModel infoModel : usedInfoMap.values()) {
            if (infoModel.getDetailInfo() != null) {
                allDetailInfo.putAll(infoModel.getDetailInfo());
            }
        }
        for (Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> entry : allDetailInfo.entrySet()) {
            if (!entry.getKey().isActive()) continue;
            KeeperUsedInfo activeKeeperUsedInfo = entry.getValue();
            String backUpKeeperIp = getBackUpKeeperIp(entry.getKey());
            if (backUpKeeperIp == null) return false;
            addIpPair(activeKeeperUsedInfo.getKeeperIP(), backUpKeeperIp, entry);
        }
        return true;
    }

    @Override
    public String getBackUpKeeperIp(DcClusterShard activeKeeper) {
        KeeperUsedInfo backUpKeeperUsedInfo = allDetailInfo.get(new DcClusterShardKeeper(activeKeeper, false));
        if (backUpKeeperUsedInfo == null) {
            CatEventMonitor.DEFAULT.logEvent(KEEPER_NO_BACKUP, activeKeeper.toString());
            logger.warn("[analyzeKeeperPair] active keeper {} has no backup keeper", activeKeeper);
            return null;
        }
        return backUpKeeperUsedInfo.getKeeperIP();
    }

    @Override
    public List<String> getAllPairsIP(String ip) {
        List<String> ipPairs = new ArrayList<>();
        if (ipPairMap.containsKey(ip)) {
            ipPairs.addAll(ipPairMap.get(ip).keySet());
        }
        return ipPairs;
    }

    @Override
    public IPPairData getIPPairData(String ip1, String ip2) {
        if (ipPairMap.containsKey(ip1) && ipPairMap.get(ip1).containsKey(ip2)) {
            return ipPairMap.get(ip1).get(ip2);
        }
        if (ipPairMap.containsKey(ip2) && ipPairMap.get(ip2).containsKey(ip1)) {
            return ipPairMap.get(ip2).get(ip1);
        }
        return null;
    }

    @Override
    public Map<DcClusterShardKeeper, KeeperUsedInfo> getAllDetailInfo(String ip1, String ip2) {
        return ipPairMap.get(ip1).get(ip2).getKeeperUsedInfoMap();
    }

    @Override
    public void updateMigrateIpPair(String srcKeeperIp, String srcKeeperIpPair, String targetKeeperIp, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> migrateDcClusterShard) {
        removeIpPair(srcKeeperIp, srcKeeperIpPair, migrateDcClusterShard);
        addIpPair(srcKeeperIpPair, targetKeeperIp, migrateDcClusterShard);
    }

    private void addIpPair(String ip1, String ip2, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> migrateDcClusterShard) {
        ipPairMap.computeIfAbsent(ip1, k -> new HashMap<>());
        ipPairMap.get(ip1).computeIfAbsent(ip2, k -> new IPPairData());
        ipPairMap.get(ip1).get(ip2).addDcClusterShard(migrateDcClusterShard);
        ipPairMap.computeIfAbsent(ip2, k -> new HashMap<>());
        ipPairMap.get(ip2).computeIfAbsent(ip1, k -> new IPPairData());
        ipPairMap.get(ip2).get(ip1).addDcClusterShard(migrateDcClusterShard);
    }

    private void removeIpPair(String ip1, String ip2, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> migrateDcClusterShard) {
        if (ipPairMap.containsKey(ip1) && ipPairMap.get(ip1).containsKey(ip2)) {
            ipPairMap.get(ip1).get(ip2).removeDcClusterShard(migrateDcClusterShard);
        }
        if (ipPairMap.containsKey(ip2) && ipPairMap.get(ip2).containsKey(ip1)) {
            ipPairMap.get(ip2).get(ip1).removeDcClusterShard(migrateDcClusterShard);
        }
    }

}
