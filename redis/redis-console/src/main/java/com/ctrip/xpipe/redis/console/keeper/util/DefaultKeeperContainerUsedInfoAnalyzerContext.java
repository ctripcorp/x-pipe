package com.ctrip.xpipe.redis.console.keeper.util;

import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;
import com.ctrip.xpipe.redis.console.keeper.handler.KeeperContainerFilterChain;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.utils.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.stream.Collectors;

public class DefaultKeeperContainerUsedInfoAnalyzerContext implements KeeperContainerUsedInfoAnalyzerContext {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerUsedInfoAnalyzerContext.class);
    private Map<String, Map<String , IPPairData>> ipPairMap = new HashMap<>();
    private Map<DcClusterShardKeeper, KeeperUsedInfo> allDetailInfo = new HashMap<>();

    private PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers;

    private PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers;

    private Map<String, List<MigrationKeeperContainerDetailModel>> migrationPlansMap = new HashMap<>();

    private KeeperContainerFilterChain filterChain;

    private static final String KEEPER_NO_BACKUP = "keeper_no_backup";

    public DefaultKeeperContainerUsedInfoAnalyzerContext(KeeperContainerFilterChain filterChain) {
        this.filterChain = filterChain;
    }

    @Override
    public void addMigrationPlan(KeeperContainerUsedInfoModel src, KeeperContainerUsedInfoModel target, boolean switchActive, boolean keeperPairOverload, String cause, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> dcClusterShard, KeeperContainerUsedInfoModel srcPair) {
        if (!migrationPlansMap.containsKey(src.getKeeperIp())) {
            migrationPlansMap.put(src.getKeeperIp(), new ArrayList<>());
        }
        if (migrationPlansMap.get(src.getKeeperIp()).stream().noneMatch(list -> list.isKeeperPairOverload() == keeperPairOverload && list.isSwitchActive() == switchActive && target == list.getTargetKeeperContainer() && StringUtils.equals(cause, list.getCause()))) {
            migrationPlansMap.get(src.getKeeperIp()).add(new MigrationKeeperContainerDetailModel(src, target, switchActive, keeperPairOverload, cause, new ArrayList<>()));
        }
        MigrationKeeperContainerDetailModel model = migrationPlansMap.get(src.getKeeperIp()).stream().filter(list -> list.isKeeperPairOverload() == keeperPairOverload && list.isSwitchActive() == switchActive && target == list.getTargetKeeperContainer() && StringUtils.equals(cause, list.getCause())).findFirst().get();
        model.addReadyToMigrateShard(dcClusterShard.getKey());

        if (!keeperPairOverload) {
            src.setActiveInputFlow(src.getActiveInputFlow() - dcClusterShard.getValue().getInputFlow()).setActiveRedisUsedMemory(src.getActiveRedisUsedMemory() - dcClusterShard.getValue().getPeerData());
            target.setActiveInputFlow(target.getActiveInputFlow() + dcClusterShard.getValue().getInputFlow()).setActiveRedisUsedMemory(target.getActiveRedisUsedMemory() + dcClusterShard.getValue().getPeerData());
        }
        if (!switchActive) {
            updateMigrateIpPair(src.getKeeperIp(), srcPair.getKeeperIp(), target.getKeeperIp(), dcClusterShard);
        }
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getAllMigrationPlans() {
        return migrationPlansMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    @Override
    public void addResourceLackPlan(KeeperContainerUsedInfoModel src, String cause) {
        if (!migrationPlansMap.containsKey(src.getKeeperIp())) {
            migrationPlansMap.put(src.getKeeperIp(), new ArrayList<>());
        }
        migrationPlansMap.get(src.getKeeperIp()).add(new MigrationKeeperContainerDetailModel(src, null, false, false, cause, new ArrayList<>()));
    }

    @Override
    public void initAvailablePool(List<KeeperContainerUsedInfoModel> usedInfoMap) {
        minInputFlowKeeperContainers = new PriorityQueue<>(usedInfoMap.size(),
                (keeper1, keeper2) -> (int)((keeper2.getInputFlowStandard() - keeper2.getActiveInputFlow()) - (keeper1.getInputFlowStandard() - keeper1.getActiveInputFlow())));
        minPeerDataKeeperContainers = new PriorityQueue<>(usedInfoMap.size(),
                (keeper1, keeper2) -> (int)((keeper2.getRedisUsedMemoryStandard() - keeper2.getTotalRedisUsedMemory()) - (keeper1.getRedisUsedMemoryStandard() - keeper1.getTotalRedisUsedMemory())));
        usedInfoMap.forEach(keeperContainerInfoModel -> {
             if (filterChain.isKeeperContainerUseful(keeperContainerInfoModel)) {
                minPeerDataKeeperContainers.add(keeperContainerInfoModel);
                minInputFlowKeeperContainers.add(keeperContainerInfoModel);
            }
        });
    }

    @Override
    public void recycleKeeperContainer(KeeperContainerUsedInfoModel keeperContainer, boolean isPeerDataOverload) {
        if (keeperContainer == null || filterChain.isDataOverLoad(keeperContainer)) return;
        PriorityQueue<KeeperContainerUsedInfoModel> queue = isPeerDataOverload ? minPeerDataKeeperContainers : minInputFlowKeeperContainers;
        PriorityQueue<KeeperContainerUsedInfoModel> anotherQueue = isPeerDataOverload ? minInputFlowKeeperContainers : minPeerDataKeeperContainers;
        queue.add(keeperContainer);
        if (anotherQueue.remove(keeperContainer)) {
            anotherQueue.add(keeperContainer);
        }
    }

    @Override
    public KeeperContainerUsedInfoModel getBestKeeperContainer(KeeperContainerUsedInfoModel usedInfoModel, KeeperContainerUsedInfoModel backUpKeeper, boolean isPeerDataOverload) {
        String org = usedInfoModel.getOrg();
        String az = usedInfoModel.getAz();
        PriorityQueue<KeeperContainerUsedInfoModel> queue = isPeerDataOverload ? minPeerDataKeeperContainers : minInputFlowKeeperContainers;
        Queue<KeeperContainerUsedInfoModel> temp = new LinkedList<>();
        while (!queue.isEmpty()) {
            KeeperContainerUsedInfoModel keeperContainer = queue.poll();
            if ((org == null || org.equals(keeperContainer.getOrg())) && (az == null || az.equals(keeperContainer.getAz())) && !keeperContainer.getKeeperIp().equals(backUpKeeper.getKeeperIp())) {
                return keeperContainer;
            }
            temp.add(keeperContainer);
        }
        while(!temp.isEmpty()) {
            queue.add(temp.poll());
        }
        return null;
    }

    @Override
    public boolean initKeeperPairData(List<KeeperContainerUsedInfoModel> usedInfoMap) {
        ipPairMap.clear();
        allDetailInfo.clear();
        for (KeeperContainerUsedInfoModel infoModel : usedInfoMap) {
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
            throw new RuntimeException("active keeper " + activeKeeper + " has no backup keeper");
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
