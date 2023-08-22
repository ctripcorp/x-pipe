package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.AbstractSiteLeaderIntervalAction;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerOverloadCause;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperDetailModel;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class DefaultKeeperContainerUsedInfoAnalyzer extends AbstractSiteLeaderIntervalAction implements KeeperContainerUsedInfoAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerUsedInfoAnalyzer.class);

    private ConsoleConfig config;

    private Set<Integer> checkerIndexes = new HashSet<>();

    private List<KeeperContainerInfoModel> allKeeperContainerInfoModels = new ArrayList<>();

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    public List<MigrationKeeperContainerDetailModel> getAllDcReadyToMigrationKeeperContainers() {
        List<KeeperContainerInfoModel> newKeeperContainerInfoModels;
        synchronized (this) {
            if (checkerIndexes.size() != config.getClusterDividedParts()) {
                logger.warn("[getAllDcReadyToMigrationKeeperContainers] the info of keeper container is incomplete: {}", checkerIndexes);
                return null;
            }
            newKeeperContainerInfoModels = Lists.newArrayList(allKeeperContainerInfoModels);
            allKeeperContainerInfoModels.clear();
            checkerIndexes.clear();
        }

        PriorityQueue<KeeperContainerInfoModel> minInputFlowKeeperContainers = new PriorityQueue<>(newKeeperContainerInfoModels.size(),
                (keeper1, keeper2) -> (int)(keeper1.getTotalInputFlow() - keeper2.getTotalInputFlow()));
        PriorityQueue<KeeperContainerInfoModel> minPeerDataKeeperContainers = new PriorityQueue<>(newKeeperContainerInfoModels.size(),
                (keeper1, keeper2) -> (int)(keeper1.getTotalRedisUsedMemory() - keeper2.getTotalRedisUsedMemory()));
        newKeeperContainerInfoModels.forEach(keeperContainerInfoModel -> {
            minPeerDataKeeperContainers.add(keeperContainerInfoModel);
            minInputFlowKeeperContainers.add(keeperContainerInfoModel);
        });

        Map<String, KeeperContainerOverloadStandardModel> keeperContainerOverloadStandards = config.getKeeperContainerOverloadStandards();
        KeeperContainerOverloadStandardModel keeperContainerOverloadStandard = keeperContainerOverloadStandards.get(currentDc);
        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        for (KeeperContainerInfoModel keeperContainerInfoModel : newKeeperContainerInfoModels) {
            long overloadInputFlow = keeperContainerInfoModel.getTotalInputFlow() - keeperContainerOverloadStandard.getFlowOverload();
            long overloadPeerData = keeperContainerInfoModel.getTotalRedisUsedMemory() - keeperContainerOverloadStandard.getPeerDataOverload();

            KeeperContainerOverloadCause overloadCause = getKeeperContainerOverloadCause(overloadInputFlow, overloadPeerData);
            if (overloadCause == null) continue;

            MigrationKeeperContainerDetailModel detail = new MigrationKeeperContainerDetailModel();
            detail.setSrcKeeperContainerIp(keeperContainerInfoModel.getKeeperIp()).setOverloadCause(overloadCause)
                    .setMigrationKeeperDetails(getMigrationKeeperDetails(keeperContainerInfoModel, overloadInputFlow,
                            overloadPeerData, overloadCause, keeperContainerOverloadStandard, minInputFlowKeeperContainers, minPeerDataKeeperContainers));

            result.add(detail);
        }
        return result;
    }

    private List<MigrationKeeperDetailModel> getMigrationKeeperDetails(KeeperContainerInfoModel srcKeeperContainer,
           long overloadInputFlow, long overloadPeerData, KeeperContainerOverloadCause overloadCause,
           KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
           PriorityQueue<KeeperContainerInfoModel> minInputFlowKeeperContainers,
           PriorityQueue<KeeperContainerInfoModel> minPeerDataKeeperContainers) {

        switch (overloadCause) {
            case PEER_DATA_OVERLoad:
                return getMigrationKeeperDetailsByPeerData(srcKeeperContainer, overloadPeerData, keeperContainerOverloadStandard, minPeerDataKeeperContainers);
            case INPUT_FLOW_OVERLOAD:
            case BOTH:
                return getMigrationKeeperDetailsByInputFlow(srcKeeperContainer, overloadInputFlow, keeperContainerOverloadStandard, minInputFlowKeeperContainers);
            default:
                logger.warn("invalid keeper container overload cause {}", overloadCause);
                return null;
        }
    }

    private List<MigrationKeeperDetailModel> getMigrationKeeperDetailsByInputFlow(KeeperContainerInfoModel srcKeeperContainer,
                  long overloadInputFlow, KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                  PriorityQueue<KeeperContainerInfoModel> minInputFlowKeeperContainers) {

        List<Map.Entry<DcClusterShard, Pair<Long, Long>>> allDcClusterShards = srcKeeperContainer.getDetailInfo().entrySet().stream()
                .sorted((o1, o2) -> (int) (o2.getValue().getKey() - o1.getValue().getKey())).collect(Collectors.toList());

        List<MigrationKeeperDetailModel> result = new ArrayList<>();
        for (Map.Entry<DcClusterShard, Pair<Long, Long>> dcClusterShard : allDcClusterShards) {
            KeeperContainerInfoModel target = minInputFlowKeeperContainers.poll();
            if (target == null) {
                logger.debug("[getMigrationKeeperDetailsByInputFlow] no available keeper containers {} for overload keeper container {}",
                        minInputFlowKeeperContainers, srcKeeperContainer);
                break;
            }
            long targetInputFlow = target.getTotalInputFlow() + dcClusterShard.getValue().getKey();
            if (targetInputFlow > keeperContainerOverloadStandard.getFlowOverload()) {
                logger.debug("[getMigrationKeeperDetailsByInputFlow] target keeper container {} can not add shard {}",
                        target, dcClusterShard);
                break;
            }
            long targetPeerData = target.getTotalRedisUsedMemory() + dcClusterShard.getValue().getValue();
            if (targetPeerData > keeperContainerOverloadStandard.getPeerDataOverload()) continue;

            result.add(new MigrationKeeperDetailModel().setMigrateShad(dcClusterShard.getKey())
                    .setTargetKeeperContainerIp(target.getKeeperIp()));
            target.setTotalInputFlow(targetInputFlow).setTotalRedisUsedMemory(targetPeerData);
            minInputFlowKeeperContainers.add(target);

            if ((overloadInputFlow =- dcClusterShard.getValue().getKey()) < 0) break;
        }

        return result;
    }

    private List<MigrationKeeperDetailModel> getMigrationKeeperDetailsByPeerData(KeeperContainerInfoModel srcKeeperContainer,
                             long overloadPeerData, KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                              PriorityQueue<KeeperContainerInfoModel> minPeerDataKeeperContainers) {

        List<Map.Entry<DcClusterShard, Pair<Long, Long>>> allDcClusterShards = srcKeeperContainer.getDetailInfo().entrySet().stream()
                .sorted((o1, o2) -> (int) (o2.getValue().getValue() - o1.getValue().getValue())).collect(Collectors.toList());

        List<MigrationKeeperDetailModel> result = new ArrayList<>();
        for (Map.Entry<DcClusterShard, Pair<Long, Long>> dcClusterShard : allDcClusterShards) {
            KeeperContainerInfoModel target = minPeerDataKeeperContainers.poll();
            if (target == null) {
                logger.debug("[getMigrationKeeperDetailsByInputFlow] no available keeper containers {} for overload keeper container {}",
                        minPeerDataKeeperContainers, srcKeeperContainer);
                break;
            }
            long targetPeerData = target.getTotalRedisUsedMemory() + dcClusterShard.getValue().getValue();
            if (targetPeerData > keeperContainerOverloadStandard.getPeerDataOverload()) {
                logger.debug("[getMigrationKeeperDetailsByInputFlow] target keeper container {} can not add shard {}",
                        target, dcClusterShard);
                break;
            }
            long targetInputFlow = target.getTotalInputFlow() + dcClusterShard.getValue().getKey();
            if (targetInputFlow > keeperContainerOverloadStandard.getFlowOverload()) continue;

            result.add(new MigrationKeeperDetailModel().setMigrateShad(dcClusterShard.getKey())
                    .setTargetKeeperContainerIp(target.getKeeperIp()));
            target.setTotalInputFlow(targetInputFlow).setTotalRedisUsedMemory(targetPeerData);
            minPeerDataKeeperContainers.add(target);

            if ((overloadPeerData =- dcClusterShard.getValue().getKey()) < 0) break;
        }

        return result;
    }

    private KeeperContainerOverloadCause getKeeperContainerOverloadCause(long overloadInputFlow, long overloadPeerData) {
        if (overloadInputFlow < 0 && overloadPeerData < 0) {
            return null;
        } else if (overloadInputFlow > 0 && overloadPeerData > 0) {
            return KeeperContainerOverloadCause.BOTH;
        } else if (overloadInputFlow > 0) {
            return KeeperContainerOverloadCause.INPUT_FLOW_OVERLOAD;
        } else {
            return KeeperContainerOverloadCause.PEER_DATA_OVERLoad;
        }
    }

    @Override
    public synchronized void updateKeeperContainerUsedInfo(int index, List<KeeperContainerInfoModel> keeperContainerInfoModels) {
        checkerIndexes.add(index);
        allKeeperContainerInfoModels.addAll(keeperContainerInfoModels);
    }

    @Override
    protected void doAction() {

    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return null;
    }
}
