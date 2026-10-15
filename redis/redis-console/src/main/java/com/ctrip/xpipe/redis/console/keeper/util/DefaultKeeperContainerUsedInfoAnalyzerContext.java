package com.ctrip.xpipe.redis.console.keeper.util;

import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.KeeperUsedInfo;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;
import com.ctrip.xpipe.redis.console.keeper.handler.KeeperContainerFilterChain;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DefaultKeeperContainerUsedInfoAnalyzerContext implements KeeperContainerUsedInfoAnalyzerContext {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerUsedInfoAnalyzerContext.class);
    private Map<Long, Map<Long, IPPairData>> containerPairMap = new HashMap<>();
    private Map<DcClusterShardKeeper, KeeperUsedInfo> allDetailInfo = new HashMap<>();

    private PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers;

    private PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers;

    private Map<Long, List<MigrationKeeperContainerDetailModel>> migrationPlansMap = new HashMap<>();

    private Set<Long> problemKeeperContainer = new HashSet<>();

    private Map<Long, KeeperContainerUsedInfoModel> srcModelsMap;

    private KeeperContainerFilterChain filterChain;

    private DcMeta dcMeta;

    private static final String KEEPER_NO_BACKUP = "keeper_no_backup";

    public DefaultKeeperContainerUsedInfoAnalyzerContext(KeeperContainerFilterChain filterChain, DcMeta dcMeta) {
        this.filterChain = filterChain;
        this.dcMeta = dcMeta;
        if (dcMeta == null) {
            logger.warn("[DefaultKeeperContainerUsedInfoAnalyzerContext dcMeta is null!");
        }
    }

    @Override
    public void addMigrationPlan(KeeperContainerUsedInfoModel src, KeeperContainerUsedInfoModel target, boolean switchActive, boolean keeperPairOverload, String cause, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> dcClusterShard, KeeperContainerUsedInfoModel srcPair) {
        Long srcContainerId = src.getKeeperContainerId();
        Long targetContainerId = target.getKeeperContainerId();
        
        if (!migrationPlansMap.containsKey(srcContainerId)) {
            migrationPlansMap.put(srcContainerId, new ArrayList<>());
        }
        if (migrationPlansMap.get(srcContainerId).stream().noneMatch(list -> list.isKeeperPairOverload() == keeperPairOverload && list.isSwitchActive() == switchActive && list.getTargetKeeperContainer() != null && targetContainerId.equals(list.getTargetKeeperContainer().getKeeperContainerId()) && StringUtils.equals(cause, list.getCause()))) {
            MigrationKeeperContainerDetailModel model = new MigrationKeeperContainerDetailModel(
                    srcModelsMap.get(srcContainerId), srcModelsMap.get(targetContainerId), switchActive, keeperPairOverload, cause, new ArrayList<>());
            if (keeperPairOverload) {
                // 这个字段是给页面展示用的地址标识（物理机 IP / 容器 hostname），不是 id
                model.setSrcOverLoadKeeperPairIp(srcPair.ipOrHostname());
            }
            migrationPlansMap.get(srcContainerId).add(model);
        }
        MigrationKeeperContainerDetailModel model = migrationPlansMap.get(srcContainerId).stream().filter(list -> list.isKeeperPairOverload() == keeperPairOverload && list.isSwitchActive() == switchActive && list.getTargetKeeperContainer() != null && targetContainerId.equals(list.getTargetKeeperContainer().getKeeperContainerId()) && StringUtils.equals(cause, list.getCause())).findFirst().get();
        model.addReadyToMigrateShard(dcClusterShard.getKey());

        if (!keeperPairOverload) {
            src.setActiveInputFlow(src.getActiveInputFlow() - dcClusterShard.getValue().getInputFlow()).setActiveRedisUsedMemory(src.getActiveRedisUsedMemory() - dcClusterShard.getValue().getPeerData());
            target.setActiveInputFlow(target.getActiveInputFlow() + dcClusterShard.getValue().getInputFlow()).setActiveRedisUsedMemory(target.getActiveRedisUsedMemory() + dcClusterShard.getValue().getPeerData());
        }
        if (!switchActive) {
            updateMigrateContainerPair(srcContainerId, srcPair.getKeeperContainerId(), targetContainerId, dcClusterShard);
        }
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getAllMigrationPlans() {
        return migrationPlansMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    @Override
    public void addResourceLackPlan(KeeperContainerUsedInfoModel src, String srcOverLoadKeeperPairIp, String cause) {
        Long srcContainerId = src.getKeeperContainerId();
        if (!migrationPlansMap.containsKey(srcContainerId)) {
            migrationPlansMap.put(srcContainerId, new ArrayList<>());
        }
        MigrationKeeperContainerDetailModel model = new MigrationKeeperContainerDetailModel(srcModelsMap.get(srcContainerId), null, false, false, cause, new ArrayList<>())
                .setUpdateTime(new Date(System.currentTimeMillis() + 8 * 60 * 60 * 1000));
        if (srcOverLoadKeeperPairIp != null) {
            model.setSrcOverLoadKeeperPairIp(srcOverLoadKeeperPairIp);
        }
        migrationPlansMap.get(srcContainerId).add(model);
    }
    
    @Override
    public boolean isProblemKeeperContainerById(Long keeperContainerId) {
        return problemKeeperContainer.contains(keeperContainerId);
    }

    @Override
    public void initAvailablePool(List<KeeperContainerUsedInfoModel> usedInfoMap) {
        minInputFlowKeeperContainers = new PriorityQueue<>(usedInfoMap.size(),
                (keeper1, keeper2) -> (int)((keeper2.getInputFlowStandard() - keeper2.getActiveInputFlow()) - (keeper1.getInputFlowStandard() - keeper1.getActiveInputFlow())));
        minPeerDataKeeperContainers = new PriorityQueue<>(usedInfoMap.size(),
                (keeper1, keeper2) -> (int)((keeper2.getRedisUsedMemoryStandard() - keeper2.getTotalRedisUsedMemory()) - (keeper1.getRedisUsedMemoryStandard() - keeper1.getTotalRedisUsedMemory())));
        usedInfoMap.forEach(model -> {
             if (!problemKeeperContainer.contains(model.getKeeperContainerId())
                     && filterChain.isKeeperContainerUseful(model)) {
                minPeerDataKeeperContainers.add(model);
                minInputFlowKeeperContainers.add(model);
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
    public KeeperContainerUsedInfoModel getBestKeeperContainer(KeeperContainerUsedInfoModel srcKeeper, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> dcClusterShard, KeeperContainerUsedInfoModel srcKeeperPair, boolean isPeerDataOverload, boolean isMigrateShardBackUp) {
        if (srcKeeper == null || srcKeeperPair == null) {
            return null;
        }
        Long srcKeeperPairId = srcKeeperPair.getKeeperContainerId();
        PriorityQueue<KeeperContainerUsedInfoModel> queue = isPeerDataOverload ? minPeerDataKeeperContainers : minInputFlowKeeperContainers;
        Queue<KeeperContainerUsedInfoModel> temp = new LinkedList<>();
        while (!queue.isEmpty()) {
            KeeperContainerUsedInfoModel target = queue.poll();
            if ((Objects.equals(srcKeeper.getOrg(), target.getOrg()))
                    && (Objects.equals(srcKeeper.getAz(), target.getAz()))
                    && (Objects.equals(srcKeeper.getTag(), target.getTag()))
                    && !Objects.equals(target.getKeeperContainerId(), srcKeeperPairId)
                    && ((!isMigrateShardBackUp && filterChain.canMigrate(dcClusterShard, srcKeeperPair, target, this))
                    || (isMigrateShardBackUp && !filterChain.isMigrateKeeperPairOverload(dcClusterShard, srcKeeperPair, target, this)))) {
                return target;
            }
            temp.add(target);
        }
        while(!temp.isEmpty()) {
            queue.add(temp.poll());
        }
        return null;
    }

    @Override
    public void initKeeperPairData(List<KeeperContainerUsedInfoModel> usedInfoMap, Map<Long, KeeperContainerUsedInfoModel> srcModelsMap) {
        this.srcModelsMap = srcModelsMap;
        containerPairMap.clear();
        allDetailInfo.clear();
        for (KeeperContainerUsedInfoModel infoModel : usedInfoMap) {
            if (infoModel.getDetailInfo() != null) {
                allDetailInfo.putAll(infoModel.getDetailInfo());
            }
        }
        for (Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> entry : allDetailInfo.entrySet()) {
            if (!entry.getKey().isActive()) continue;
            KeeperUsedInfo activeKeeperUsedInfo = entry.getValue();
            Long backUpKeeperContainerId = getBackUpKeeperContainerId(entry.getKey());
            if (backUpKeeperContainerId == null) {
                getProblemKeeperContainer(entry);
                continue;
            }
            addContainerPair(activeKeeperUsedInfo.getKeeperContainerId(), backUpKeeperContainerId, entry);
        }
    }

    private void getProblemKeeperContainer(Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> entry) {
        if (dcMeta == null) {
            return;
        }
        dcMeta.getClusters().values().forEach(clusterMeta -> {
            if (entry.getKey().getClusterId().equals(clusterMeta.getId())) {
                clusterMeta.getShards().values().forEach(shardMeta -> {
                    if (entry.getKey().getShardId().equals(shardMeta.getId())) {
                        List<KeeperMeta> keepers = shardMeta.getKeepers();
                        if (keepers.size() == 2) {
                            markProblemKeeperContainer(entry, keepers);
                        }
                    }
                });
            }
        });
    }

    /**
     * active keeper 有数据上报但配对的 backup 没有，说明 backup 所在的 keeperContainer 有问题，
     * 把它标记出来，后续不再作为迁移目标。
     * <p>
     * 两个 keeper 里取与 active 不同的那一个；任一 id 缺失时无法判定是哪一个，
     * 此时宁可放弃标记也不能瞎猜——猜错会把健康的 active 容器拉黑。
     */
    private void markProblemKeeperContainer(Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> entry, List<KeeperMeta> keepers) {
        Long activeContainerId = entry.getValue().getKeeperContainerId();
        Long firstContainerId = keepers.get(0).getKeeperContainerId();
        Long secondContainerId = keepers.get(1).getKeeperContainerId();

        if (activeContainerId == null || firstContainerId == null || secondContainerId == null) {
            logger.warn("[markProblemKeeperContainer][keeperContainerId missing] {}, activeId:{}, keeperIds:[{}, {}]",
                    entry.getKey(), activeContainerId, firstContainerId, secondContainerId);
            return;
        }

        Long problemContainerId = activeContainerId.equals(firstContainerId) ? secondContainerId : firstContainerId;
        problemKeeperContainer.add(problemContainerId);
    }

    @Override
    public Long getBackUpKeeperContainerId(DcClusterShard activeKeeper) {
        KeeperUsedInfo backUpKeeperUsedInfo = allDetailInfo.get(new DcClusterShardKeeper(activeKeeper, false));
        if (backUpKeeperUsedInfo == null) {
            CatEventMonitor.DEFAULT.logEvent(KEEPER_NO_BACKUP, activeKeeper.toString());
            return null;
        }
        return backUpKeeperUsedInfo.getKeeperContainerId();
    }

    @Override
    public List<Long> getAllPairsContainerId(Long containerId) {
        List<Long> pairIds = new ArrayList<>();
        if (containerPairMap.containsKey(containerId)) {
            pairIds.addAll(containerPairMap.get(containerId).keySet());
        }
        return pairIds;
    }

    @Override
    public IPPairData getContainerPairData(Long containerId1, Long containerId2) {
        if (containerPairMap.containsKey(containerId1) && containerPairMap.get(containerId1).containsKey(containerId2)) {
            return containerPairMap.get(containerId1).get(containerId2);
        }
        if (containerPairMap.containsKey(containerId2) && containerPairMap.get(containerId2).containsKey(containerId1)) {
            return containerPairMap.get(containerId2).get(containerId1);
        }
        return null;
    }
    
    public void updateMigrateContainerPair(Long srcKeeperContainerId, Long srcKeeperPairContainerId, Long targetKeeperContainerId, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> migrateDcClusterShard) {
        removeContainerPair(srcKeeperContainerId, srcKeeperPairContainerId, migrateDcClusterShard);
        addContainerPair(srcKeeperPairContainerId, targetKeeperContainerId, migrateDcClusterShard);
    }

    private void addContainerPair(Long containerId1, Long containerId2, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> migrateDcClusterShard) {
        containerPairMap.computeIfAbsent(containerId1, k -> new HashMap<>());
        containerPairMap.get(containerId1).computeIfAbsent(containerId2, k -> new IPPairData());
        containerPairMap.get(containerId1).get(containerId2).addDcClusterShard(migrateDcClusterShard);
        containerPairMap.computeIfAbsent(containerId2, k -> new HashMap<>());
        containerPairMap.get(containerId2).computeIfAbsent(containerId1, k -> new IPPairData());
        containerPairMap.get(containerId2).get(containerId1).addDcClusterShard(migrateDcClusterShard);
    }

    private void removeContainerPair(Long containerId1, Long containerId2, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> migrateDcClusterShard) {
        if (containerPairMap.containsKey(containerId1) && containerPairMap.get(containerId1).containsKey(containerId2)) {
            containerPairMap.get(containerId1).get(containerId2).removeDcClusterShard(migrateDcClusterShard);
        }
        if (containerPairMap.containsKey(containerId2) && containerPairMap.get(containerId2).containsKey(containerId1)) {
            containerPairMap.get(containerId2).get(containerId1).removeDcClusterShard(migrateDcClusterShard);
        }
    }

}
