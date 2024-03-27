package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerOverloadCause;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;
import com.ctrip.xpipe.redis.console.keeper.handler.KeeperContainerFilterChain;
import com.ctrip.xpipe.redis.console.keeper.util.DefaultKeeperContainerUsedInfoAnalyzerContext;
import com.ctrip.xpipe.redis.console.keeper.util.KeeperContainerUsedInfoAnalyzerContext;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.KeeperContainerAnalyzerService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class DefaultKeeperContainerMigrationAnalyzer implements KeeperContainerMigrationAnalyzer{

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerMigrationAnalyzer.class);

    @Autowired
    private KeeperContainerAnalyzerService keeperContainerAnalyzerService;

    @Autowired
    private KeeperContainerFilterChain filterChain;

    @Autowired
    private ConsoleConfig config;

    @Autowired
    private MetaCache metaCache;

    private KeeperContainerUsedInfoAnalyzerContext analyzerContext;

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();

    private static final String KEEPER_RESOURCE_LACK = "keeper_resource_lack";

    private static final String KEEPER_PAIR_RESOURCE_LACK = "keeper_pair_resource_lack";

    @Override
    public List<MigrationKeeperContainerDetailModel> getMigrationPlans(Map<String, KeeperContainerUsedInfoModel> modelsMap) {
        List<KeeperContainerUsedInfoModel> models = new ArrayList<>(modelsMap.values());
        keeperContainerAnalyzerService.initStandard(models);
        List<KeeperContainerUsedInfoModel> modelsWithoutResource = new ArrayList<>();
        models.forEach(a -> modelsWithoutResource.add(KeeperContainerUsedInfoModel.cloneKeeperContainerUsedInfoModel(a)));
        analyzerContext = new DefaultKeeperContainerUsedInfoAnalyzerContext(filterChain, metaCache.getXpipeMeta().getDcs().get(currentDc));
        analyzerContext.initKeeperPairData(modelsWithoutResource, modelsMap);
        analyzerContext.initAvailablePool(modelsWithoutResource);
        for (KeeperContainerUsedInfoModel model : modelsWithoutResource) {
            generateDataOverLoadMigrationPlans(model, modelsMap);
            for (String ip : analyzerContext.getAllPairsIP(model.getKeeperIp())) {
                generatePairOverLoadMigrationPlans(model, modelsMap.get(ip));
            }
        }
        return analyzerContext.getAllMigrationPlans();
    }

    private void generateDataOverLoadMigrationPlans(KeeperContainerUsedInfoModel model, Map<String, KeeperContainerUsedInfoModel> modelsMap) {
        Object[] cause = getDataOverloadCause(model);
        if (cause == null) return;
        List<Map.Entry<DcClusterShardKeeper, KeeperUsedInfo>> descShards = getDescShards(model.getDetailInfo(), (Boolean) cause[1]);
        Iterator<Map.Entry<DcClusterShardKeeper, KeeperUsedInfo>> iterator = descShards.iterator();
        while (filterChain.isDataOverLoad(model) && iterator.hasNext()) {
            Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> dcClusterShard = iterator.next();
            if (!dcClusterShard.getKey().isActive()) continue;
            KeeperContainerUsedInfoModel backUpKeeper = modelsMap.get(analyzerContext.getBackUpKeeperIp(dcClusterShard.getKey()));
            if (canSwitchMaster(model, backUpKeeper, dcClusterShard)) {
                analyzerContext.addMigrationPlan(model, backUpKeeper, true, false, (String) cause[0], dcClusterShard, null);
                continue;
            }
            KeeperContainerUsedInfoModel bestKeeperContainer = analyzerContext.getBestKeeperContainer(model, dcClusterShard, backUpKeeper, (Boolean) cause[1], false);
            if (bestKeeperContainer == null) {
                break;
            }
            analyzerContext.addMigrationPlan(model, bestKeeperContainer, false, false, (String) cause[0], dcClusterShard, backUpKeeper);
            analyzerContext.recycleKeeperContainer(bestKeeperContainer, (Boolean) cause[1]);
        }
        if (filterChain.isDataOverLoad(model)) {
            logger.warn("[analyzeKeeperContainerUsedInfo] no available space for overload keeperContainer to migrate {}", model);
            CatEventMonitor.DEFAULT.logEvent(KEEPER_RESOURCE_LACK, "Dc:" + currentDc + " Org:" + model.getOrg() + " Az:" + model.getAz());
            analyzerContext.addResourceLackPlan(model, null, KeeperContainerOverloadCause.RESOURCE_LACK.name());
        }
    }

    private void generatePairOverLoadMigrationPlans(KeeperContainerUsedInfoModel modelA, KeeperContainerUsedInfoModel modelB) {
        Object[] cause = getPariOverloadCause(modelA, modelB);
        if (cause == null) return;
        List<Map.Entry<DcClusterShardKeeper, KeeperUsedInfo>> descShards = getDescShards(analyzerContext.getIPPairData(modelA.getKeeperIp(), modelB.getKeeperIp()).getEntryMap(), (Boolean) cause[1]);
        Iterator<Map.Entry<DcClusterShardKeeper, KeeperUsedInfo>> iterator = descShards.iterator();
        while (filterChain.isKeeperContainerPairOverload(modelA, modelB, analyzerContext.getIPPairData(modelA.getKeeperIp(), modelB.getKeeperIp())) && iterator.hasNext()) {
            Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> dcClusterShard = iterator.next();
            KeeperContainerUsedInfoModel activeKeeperContainer = dcClusterShard.getValue().getKeeperIP().equals(modelA.getKeeperIp()) ? modelA : modelB;
            KeeperContainerUsedInfoModel backUpKeeperContainer = dcClusterShard.getValue().getKeeperIP().equals(modelA.getKeeperIp()) ? modelB : modelA;
            KeeperContainerUsedInfoModel bestKeeperContainer = analyzerContext.getBestKeeperContainer(backUpKeeperContainer, dcClusterShard, activeKeeperContainer, (Boolean) cause[1], true);
            if (bestKeeperContainer == null) {
                break;
            }
            if (!filterChain.isMigrateKeeperPairOverload(dcClusterShard, backUpKeeperContainer, bestKeeperContainer, analyzerContext)) {
                analyzerContext.addMigrationPlan(backUpKeeperContainer, bestKeeperContainer, false, true, (String) cause[0], dcClusterShard, activeKeeperContainer);
                analyzerContext.recycleKeeperContainer(bestKeeperContainer, (Boolean) cause[1]);
            }
        }
        if (filterChain.isKeeperContainerPairOverload(modelA, modelB, analyzerContext.getIPPairData(modelA.getKeeperIp(), modelB.getKeeperIp()))) {
            logger.warn("[analyzeKeeperContainerUsedInfo] no available space for overload keeperContainer pair to migrate {} {}", modelA, modelB);
            CatEventMonitor.DEFAULT.logEvent(KEEPER_PAIR_RESOURCE_LACK, "Dc:" + currentDc + " OrgA:" + modelA.getOrg() + " AzA:" + modelA.getAz() + " OrgB:" + modelB.getOrg() + " AzB:" + modelB.getAz());
            analyzerContext.addResourceLackPlan(modelA, modelB.getKeeperIp(),KeeperContainerOverloadCause.PAIR_RESOURCE_LACK.name());
        }
    }

    private List<Map.Entry<DcClusterShardKeeper, KeeperUsedInfo>> getDescShards(Map<DcClusterShardKeeper, KeeperUsedInfo> allDetailInfo, boolean isPeerDataOverload) {
        Comparator<Map.Entry<DcClusterShardKeeper, KeeperUsedInfo>> comparator = isPeerDataOverload ?
                Comparator.comparingLong(e -> e.getValue().getPeerData()) : Comparator.comparingLong(e -> e.getValue().getInputFlow());
        return allDetailInfo.entrySet().stream()
                .sorted(comparator.reversed())
                .collect(Collectors.toList());
    }

    private boolean canSwitchMaster(KeeperContainerUsedInfoModel src, KeeperContainerUsedInfoModel backUp, Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> dcClusterShard) {
        return backUp != null
                && !analyzerContext.isProblemKeeperContainer(backUp.getKeeperIp())
                && (src.getOrg() == null || src.getOrg().equals(backUp.getOrg()))
                && (src.getAz() == null || src.getAz().equals(backUp.getAz()))
                && filterChain.isKeeperContainerUseful(backUp)
                && getPariOverloadCause(src, backUp) == null
                && filterChain.canMigrate(dcClusterShard, backUp, backUp, analyzerContext);
    }

    private Object[] getDataOverloadCause(KeeperContainerUsedInfoModel infoModel) {
        long overloadInputFlow = infoModel.getActiveInputFlow() - infoModel.getInputFlowStandard();
        long overloadPeerData = infoModel.getActiveRedisUsedMemory() - infoModel.getRedisUsedMemoryStandard();
        if (overloadInputFlow <= 0 && overloadPeerData <= 0) {
            return null;
        } else if (overloadPeerData > 0 && overloadInputFlow >= overloadPeerData) {
            return new Object[] {KeeperContainerOverloadCause.BOTH.name(), false};
        } else if (overloadInputFlow > 0 && overloadInputFlow < overloadPeerData) {
            return new Object[] {KeeperContainerOverloadCause.BOTH.name(), true};
        } else if (overloadInputFlow > 0) {
            return new Object[] {KeeperContainerOverloadCause.INPUT_FLOW_OVERLOAD.name(), false};
        } else {
            return new Object[] {KeeperContainerOverloadCause.PEER_DATA_OVERLOAD.name(), true};
        }
    }

    private Object[] getPariOverloadCause(KeeperContainerUsedInfoModel pairA, KeeperContainerUsedInfoModel pairB) {
        double keeperPairOverLoadFactor = config.getKeeperPairOverLoadFactor();
        KeeperContainerOverloadStandardModel minStandardModel = new KeeperContainerOverloadStandardModel()
                .setFlowOverload((long) (Math.min(pairB.getInputFlowStandard(), pairA.getInputFlowStandard()) * keeperPairOverLoadFactor))
                .setPeerDataOverload((long) (Math.min(pairB.getRedisUsedMemoryStandard(), pairA.getRedisUsedMemoryStandard()) * keeperPairOverLoadFactor));

        IPPairData longLongPair = analyzerContext.getIPPairData(pairA.getKeeperIp(), pairB.getKeeperIp());
        long overloadInputFlow = longLongPair.getInputFlow() - minStandardModel.getFlowOverload();
        long overloadPeerData = longLongPair.getPeerData() - minStandardModel.getPeerDataOverload();
        if (overloadInputFlow <= 0 && overloadPeerData <= 0) {
            return null;
        } else if (overloadPeerData > 0 && overloadInputFlow >= overloadPeerData) {
            return new Object[] {KeeperContainerOverloadCause.KEEPER_PAIR_BOTH.name(), false};
        } else if (overloadInputFlow > 0 && overloadInputFlow < overloadPeerData) {
            return new Object[] {KeeperContainerOverloadCause.KEEPER_PAIR_BOTH.name(), true};
        } else if (overloadInputFlow > 0) {
            return new Object[] {KeeperContainerOverloadCause.KEEPER_PAIR_INPUT_FLOW_OVERLOAD.name(), false};
        } else {
            return new Object[] {KeeperContainerOverloadCause.KEEPER_PAIR_PEER_DATA_OVERLOAD.name(), true};
        }
    }

    @VisibleForTesting
    public void setKeeperContainerAnalyzerService(KeeperContainerAnalyzerService keeperContainerAnalyzerService) {
        this.keeperContainerAnalyzerService = keeperContainerAnalyzerService;
    }

    @VisibleForTesting
    public void setFilterChain(KeeperContainerFilterChain filterChain) {
        this.filterChain = filterChain;
    }

    @VisibleForTesting
    public void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }
}
