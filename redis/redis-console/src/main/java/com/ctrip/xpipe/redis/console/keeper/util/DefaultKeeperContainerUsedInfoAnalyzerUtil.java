package com.ctrip.xpipe.redis.console.keeper.util;

import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPair;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultKeeperContainerUsedInfoAnalyzerUtil implements KeeperContainerUsedInfoAnalyzerUtil{

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerUsedInfoAnalyzerUtil.class);
    private Map<IPPair, IPPairData> keeperPairUsedInfoMap = new HashMap<>();
    private Map<String, Map<String ,Map<DcClusterShardActive, KeeperUsedInfo>>> ipPairMap = new HashMap<>();
    private Map<DcClusterShardActive, KeeperUsedInfo> allDetailInfo = new HashMap<>();

    public DefaultKeeperContainerUsedInfoAnalyzerUtil() {}

    @Override
    public long getMaxActiveRedisUsedMemory(Map<String, KeeperContainerUsedInfoModel> usedInfo) {
        long max = 0;
        for (KeeperContainerUsedInfoModel usedInfoModel : usedInfo.values()) {
            max = Math.max(max, usedInfoModel.getActiveRedisUsedMemory());
        }
        return max;
    }

    @Override
    public void initKeeperPairData(Map<String, KeeperContainerUsedInfoModel> usedInfoMap) {
        keeperPairUsedInfoMap.clear();
        ipPairMap.clear();
        allDetailInfo.clear();
        for (KeeperContainerUsedInfoModel infoModel : usedInfoMap.values()) {
            if (infoModel.getDetailInfo() != null) {
                allDetailInfo.putAll(infoModel.getDetailInfo());
            }
        }
        for (Map.Entry<DcClusterShardActive, KeeperUsedInfo> entry : allDetailInfo.entrySet()) {
            if (!entry.getKey().isActive()) continue;
            KeeperUsedInfo activeKeeperUsedInfo = entry.getValue();
            String backUpKeeperIp = getBackUpKeeperIp(entry.getKey());
            if (backUpKeeperIp == null) continue;
            IPPair ipPair = new IPPair(activeKeeperUsedInfo.getKeeperIP(), backUpKeeperIp);
            IPPairData ipPairData = new IPPairData(activeKeeperUsedInfo.getInputFlow(), activeKeeperUsedInfo.getPeerData(), 1);
            if (keeperPairUsedInfoMap.containsKey(ipPair)) {
                IPPairData value = keeperPairUsedInfoMap.get(ipPair);
                ipPairData = new IPPairData(activeKeeperUsedInfo.getInputFlow() + value.getInputFlow(), activeKeeperUsedInfo.getPeerData() + value.getPeerData(), value.getNumber() + 1);
            }
            keeperPairUsedInfoMap.put(ipPair, ipPairData);

            generateIpPairMap(entry, activeKeeperUsedInfo.getKeeperIP(), backUpKeeperIp);
        }
    }

    @Override
    public String getBackUpKeeperIp(DcClusterShard activeKeeper) {
        KeeperUsedInfo backUpKeeperUsedInfo = allDetailInfo.get(new DcClusterShardActive(activeKeeper, false));
        if (backUpKeeperUsedInfo == null) {
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
        return keeperPairUsedInfoMap.get(new IPPair(ip1, ip2));
    }

    @Override
    public Map<DcClusterShardActive, KeeperUsedInfo> getAllDetailInfo(String ip1, String ip2) {
        return ipPairMap.get(ip1).get(ip2);
    }

    @Override
    public void updateMigrateIpPair(String srcKeeperIp, String srcKeeperIpPair, String targetKeeperIp, Map.Entry<DcClusterShardActive, KeeperUsedInfo> migrateDcClusterShard) {
        IPPair ipPair = new IPPair(srcKeeperIp, srcKeeperIpPair);
        IPPairData ipPairData = keeperPairUsedInfoMap.get(ipPair);
        if (ipPairData.getNumber() == 1) {
            keeperPairUsedInfoMap.remove(ipPair);
            ipPairMap.get(srcKeeperIp).remove(srcKeeperIpPair);
            ipPairMap.get(srcKeeperIpPair).remove(srcKeeperIp);
        } else {
            keeperPairUsedInfoMap.put(ipPair, ipPairData.subData(migrateDcClusterShard.getValue().getInputFlow(), migrateDcClusterShard.getValue().getPeerData()));
            ipPairMap.get(srcKeeperIp).get(srcKeeperIpPair).remove(migrateDcClusterShard.getKey());
            ipPairMap.get(srcKeeperIpPair).get(srcKeeperIp).remove(migrateDcClusterShard.getKey());
        }
        IPPair newIPpair = new IPPair(srcKeeperIp, targetKeeperIp);
        if (keeperPairUsedInfoMap.containsKey(newIPpair)) {
            IPPairData newIpPairData = keeperPairUsedInfoMap.get(newIPpair);
            keeperPairUsedInfoMap.put(newIPpair, newIpPairData.addData(migrateDcClusterShard.getValue().getInputFlow(), migrateDcClusterShard.getValue().getPeerData()));
        } else {
            generateIpPairMap(migrateDcClusterShard, srcKeeperIp, targetKeeperIp);
            keeperPairUsedInfoMap.put(newIPpair, new IPPairData(migrateDcClusterShard.getValue().getInputFlow(), migrateDcClusterShard.getValue().getPeerData(), 1));
        }
    }

    private void generateIpPairMap(Map.Entry<DcClusterShardActive, KeeperUsedInfo> entry, String keeperIp1, String keeperIp2) {
        generateSingleIpPairMap(entry, keeperIp1, keeperIp2);
        generateSingleIpPairMap(entry, keeperIp2, keeperIp1);
    }

    private void generateSingleIpPairMap(Map.Entry<DcClusterShardActive, KeeperUsedInfo> entry, String keeperIp1, String keeperIp2) {
        ipPairMap.computeIfAbsent(keeperIp1, k -> new HashMap<>());
        ipPairMap.get(keeperIp1).computeIfAbsent(keeperIp2, k -> new HashMap<>());
        ipPairMap.get(keeperIp1).get(keeperIp2).put(entry.getKey(), entry.getValue());
    }

}
