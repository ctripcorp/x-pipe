package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerOverloadCause;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class DefaultKeeperContainerUsedInfoAnalyzer extends AbstractService implements KeeperContainerUsedInfoAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerUsedInfoAnalyzer.class);

    @Autowired
    private ConsoleConfig config;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    private Set<Integer> checkerIndexes = new HashSet<>();

    private List<KeeperContainerUsedInfoModel> allKeeperContainerUsedInfoModels = new ArrayList<>();

    private List<MigrationKeeperContainerDetailModel> allDcKeeperContainerDetailModel = new ArrayList<>();

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    private static final String KEEPER_RESOURCE_LACK = "keeper_resource_lack";

    @Override
    public List<MigrationKeeperContainerDetailModel> getAllReadyToMigrationKeeperContainers() {
        List<MigrationKeeperContainerDetailModel> result = Collections.synchronizedList(new ArrayList<>());
        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        config.getConsoleDomains().forEach((dc, domains) -> {
            if (currentDc.equalsIgnoreCase(dc)) {
                result.addAll(getAllDcReadyToMigrationKeeperContainers());
            } else {
                MigrationKeeperContainerDetailInfoGetCommand command = new MigrationKeeperContainerDetailInfoGetCommand(domains, restTemplate);
                command.future().addListener(commandFuture -> {
                    if (commandFuture.isSuccess() && commandFuture.get() != null) result.addAll(commandFuture.get());
                });
                commandChain.add(command);
            }
        });

        try {
            commandChain.execute().get(10, TimeUnit.SECONDS);
        } catch (Throwable th) {
            logger.warn("[getAllReadyToMigrationKeeperContainers] error:", th);
        }

        return result;
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getAllDcReadyToMigrationKeeperContainers() {
        return allDcKeeperContainerDetailModel;
//
//        List<KeeperContainerUsedInfoModel> newKeeperContainerUsedInfoModels;
//        synchronized (this) {
//            if (checkerIndexes.size() != config.getClusterDividedParts()) {
//                logger.warn("[getAllDcReadyToMigrationKeeperContainers] the info of keeper container is incomplete: {}", checkerIndexes);
//                return null;
//            }
//            newKeeperContainerUsedInfoModels = Lists.newArrayList(allKeeperContainerUsedInfoModels);
//            allKeeperContainerUsedInfoModels.clear();
//            checkerIndexes.clear();
//        }
//
//        PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers = new PriorityQueue<>(newKeeperContainerUsedInfoModels.size(),
//                (keeper1, keeper2) -> (int)(keeper1.getTotalInputFlow() - keeper2.getTotalInputFlow()));
//        PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers = new PriorityQueue<>(newKeeperContainerUsedInfoModels.size(),
//                (keeper1, keeper2) -> (int)(keeper1.getTotalRedisUsedMemory() - keeper2.getTotalRedisUsedMemory()));
//        newKeeperContainerUsedInfoModels.forEach(keeperContainerInfoModel -> {
//            minPeerDataKeeperContainers.add(keeperContainerInfoModel);
//            minInputFlowKeeperContainers.add(keeperContainerInfoModel);
//        });
//
//        Map<String, KeeperContainerOverloadStandardModel> keeperContainerOverloadStandards = config.getKeeperContainerOverloadStandards();
//        KeeperContainerOverloadStandardModel keeperContainerOverloadStandard = keeperContainerOverloadStandards.get(currentDc);
//        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
//        for (KeeperContainerUsedInfoModel keeperContainerUsedInfoModel : newKeeperContainerUsedInfoModels) {
//            long overloadInputFlow = keeperContainerUsedInfoModel.getTotalInputFlow() - keeperContainerOverloadStandard.getFlowOverload();
//            long overloadPeerData = keeperContainerUsedInfoModel.getTotalRedisUsedMemory() - keeperContainerOverloadStandard.getPeerDataOverload();
//
//            KeeperContainerOverloadCause overloadCause = getKeeperContainerOverloadCause(overloadInputFlow, overloadPeerData);
//            if (overloadCause == null) continue;
//
//            result.addAll(getMigrationKeeperDetails(keeperContainerUsedInfoModel, overloadInputFlow,
//                    overloadPeerData, overloadCause, keeperContainerOverloadStandard, minInputFlowKeeperContainers, minPeerDataKeeperContainers));
//        }
//        return result;
    }

    @Override
    public synchronized void updateKeeperContainerUsedInfo(int index, List<KeeperContainerUsedInfoModel> keeperContainerUsedInfoModels) {
        checkerIndexes.add(index);
        allKeeperContainerUsedInfoModels.addAll(keeperContainerUsedInfoModels);

        List<KeeperContainerUsedInfoModel> newKeeperContainerUsedInfoModels;
        if (checkerIndexes.size() < config.getClusterDividedParts()) {
            return;
        }

        newKeeperContainerUsedInfoModels = Lists.newArrayList(allKeeperContainerUsedInfoModels);
        allKeeperContainerUsedInfoModels.clear();
        checkerIndexes.clear();

        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                analyzeKeeperContainerUsedInfo(newKeeperContainerUsedInfoModels);
            }
        });
    }

    private void analyzeKeeperContainerUsedInfo(List<KeeperContainerUsedInfoModel> newKeeperContainerUsedInfoModels) {
        PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers = new PriorityQueue<>(newKeeperContainerUsedInfoModels.size(),
                (keeper1, keeper2) -> (int)(keeper1.getTotalInputFlow() - keeper2.getTotalInputFlow()));
        PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers = new PriorityQueue<>(newKeeperContainerUsedInfoModels.size(),
                (keeper1, keeper2) -> (int)(keeper1.getTotalRedisUsedMemory() - keeper2.getTotalRedisUsedMemory()));
        newKeeperContainerUsedInfoModels.forEach(keeperContainerInfoModel -> {
            minPeerDataKeeperContainers.add(keeperContainerInfoModel);
            minInputFlowKeeperContainers.add(keeperContainerInfoModel);
        });

        KeeperContainerOverloadStandardModel keeperContainerOverloadStandard = config.getKeeperContainerOverloadStandards().get(currentDc);
        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        for (KeeperContainerUsedInfoModel keeperContainerUsedInfoModel : newKeeperContainerUsedInfoModels) {
            long overloadInputFlow = keeperContainerUsedInfoModel.getTotalInputFlow() - keeperContainerOverloadStandard.getFlowOverload();
            long overloadPeerData = keeperContainerUsedInfoModel.getTotalRedisUsedMemory() - keeperContainerOverloadStandard.getPeerDataOverload();

            KeeperContainerOverloadCause overloadCause = getKeeperContainerOverloadCause(overloadInputFlow, overloadPeerData);
            if (overloadCause == null) continue;

            result.addAll(getMigrationKeeperDetails(keeperContainerUsedInfoModel, overloadInputFlow,
                    overloadPeerData, overloadCause, keeperContainerOverloadStandard, minInputFlowKeeperContainers, minPeerDataKeeperContainers));
        }
        allDcKeeperContainerDetailModel = result;
    }


    private List<MigrationKeeperContainerDetailModel> getMigrationKeeperDetails(KeeperContainerUsedInfoModel src,
                                                                                long overloadInputFlow, long overloadPeerData, KeeperContainerOverloadCause overloadCause,
                                                                                KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                                                                                PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers,
                                                                                PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers) {

        switch (overloadCause) {
            case PEER_DATA_OVERLoad:
                return getMigrationKeeperDetailsByPeerData(src, overloadPeerData, keeperContainerOverloadStandard, minPeerDataKeeperContainers);
            case INPUT_FLOW_OVERLOAD:
            case BOTH:
                return getMigrationKeeperDetailsByInputFlow(src, overloadInputFlow, keeperContainerOverloadStandard, minInputFlowKeeperContainers);
            default:
                logger.warn("invalid keeper container overload cause {}", overloadCause);
                return null;
        }
    }

    private List<MigrationKeeperContainerDetailModel> getMigrationKeeperDetailsByInputFlow(KeeperContainerUsedInfoModel src,
                                                                                           long overloadInputFlow, KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                                                                                           PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers) {

        List<Map.Entry<DcClusterShard, Pair<Long, Long>>> allDcClusterShards = src.getDetailInfo().entrySet().stream()
                .sorted((o1, o2) -> (int) (o2.getValue().getKey() - o1.getValue().getKey())).collect(Collectors.toList());

        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        KeeperContainerUsedInfoModel target = null;
        MigrationKeeperContainerDetailModel keeperContainerDetailModel = null;

        for (Map.Entry<DcClusterShard, Pair<Long, Long>> dcClusterShard : allDcClusterShards) {
            if (target == null ) {
                target = minInputFlowKeeperContainers.poll();
                if (target == null) {
                    logger.debug("[getMigrationKeeperDetailsByInputFlow] no available keeper containers {} for overload keeper container {}",
                            minInputFlowKeeperContainers, src);
                    CatEventMonitor.DEFAULT.logEvent(KEEPER_RESOURCE_LACK, currentDc);
                    return result;
                }

                keeperContainerDetailModel = new MigrationKeeperContainerDetailModel(
                        new KeeperContainerUsedInfoModel(src.getKeeperIp(),src.getDcName(), src.getTotalInputFlow(), src.getTotalRedisUsedMemory()),
                        new KeeperContainerUsedInfoModel(target.getKeeperIp(),src.getDcName(), target.getTotalInputFlow(), target.getTotalRedisUsedMemory()),
                        0, new ArrayList<>());
            }

            long targetInputFlow = target.getTotalInputFlow() + dcClusterShard.getValue().getKey();
            long targetPeerData = target.getTotalRedisUsedMemory() + dcClusterShard.getValue().getValue();
            if (targetInputFlow > keeperContainerOverloadStandard.getFlowOverload()
                    || targetPeerData > keeperContainerOverloadStandard.getPeerDataOverload()) {
                logger.debug("[getMigrationKeeperDetailsByInputFlow] target keeper container {} can not add shard {}",
                        target, dcClusterShard);
                if (keeperContainerDetailModel.getMigrateKeeperCount() != 0) result.add(keeperContainerDetailModel);
                target = null;
                keeperContainerDetailModel = null;
                continue;
            }
            keeperContainerDetailModel.getMigrateShards().add(dcClusterShard.getKey());
            keeperContainerDetailModel.migrateKeeperCountIncrease();
            result.add(keeperContainerDetailModel);
            target.setTotalInputFlow(targetInputFlow).setTotalRedisUsedMemory(targetPeerData);

            if ((overloadInputFlow =- dcClusterShard.getValue().getKey()) < 0) break;
        }

        if (target.getTotalInputFlow() < keeperContainerOverloadStandard.getPeerDataOverload()
                && target.getTotalRedisUsedMemory() < keeperContainerOverloadStandard.getPeerDataOverload()) {
            minInputFlowKeeperContainers.add(target);
        }

        return result;
    }

    private List<MigrationKeeperContainerDetailModel> getMigrationKeeperDetailsByPeerData(KeeperContainerUsedInfoModel src,
                                                                                          long overloadPeerData, KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                                                                                          PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers) {

        List<Map.Entry<DcClusterShard, Pair<Long, Long>>> allDcClusterShards = src.getDetailInfo().entrySet().stream()
                .sorted((o1, o2) -> (int) (o2.getValue().getValue() - o1.getValue().getValue())).collect(Collectors.toList());

        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        KeeperContainerUsedInfoModel target = null;

        for (Map.Entry<DcClusterShard, Pair<Long, Long>> dcClusterShard : allDcClusterShards) {
            if (target == null ) {
                target = minPeerDataKeeperContainers.poll();
                if (target == null) {
                    logger.debug("[getMigrationKeeperDetailsByPeerData] no available keeper containers {} for overload keeper container {}",
                            minPeerDataKeeperContainers, src);
                    return result;
                }
            }
            MigrationKeeperContainerDetailModel keeperContainerDetailModel = new MigrationKeeperContainerDetailModel(
                    new KeeperContainerUsedInfoModel(src.getKeeperIp(),src.getDcName(), src.getTotalInputFlow(), src.getTotalRedisUsedMemory()),
                    new KeeperContainerUsedInfoModel(target.getKeeperIp(),src.getDcName(), target.getTotalInputFlow(), target.getTotalRedisUsedMemory()),
                    0, new ArrayList<>());

            long targetPeerData = target.getTotalRedisUsedMemory() + dcClusterShard.getValue().getValue();
            long targetInputFlow = target.getTotalInputFlow() + dcClusterShard.getValue().getKey();
            if (targetPeerData > keeperContainerOverloadStandard.getPeerDataOverload()
                    || targetInputFlow > keeperContainerOverloadStandard.getFlowOverload()) {
                logger.debug("[getMigrationKeeperDetailsByInputFlow] target keeper container {} can not add shard {}", target, dcClusterShard);
                target = null;
                if (keeperContainerDetailModel.getMigrateKeeperCount() != 0) result.add(keeperContainerDetailModel);
                continue;
            }

            keeperContainerDetailModel.getMigrateShards().add(dcClusterShard.getKey());
            keeperContainerDetailModel.migrateKeeperCountIncrease();
            target.setTotalInputFlow(targetInputFlow).setTotalRedisUsedMemory(targetPeerData);
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

    class MigrationKeeperContainerDetailInfoGetCommand extends AbstractCommand<List<MigrationKeeperContainerDetailModel>> {

        private String domain;
        private RestOperations restTemplate;

        public MigrationKeeperContainerDetailInfoGetCommand(String domain, RestOperations restTemplate) {
            this.domain = domain;
            this.restTemplate = restTemplate;
        }

        @Override
        public String getName() {
            return "getMigrationKeeperContainerDetailInfo";
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                ResponseEntity<List<MigrationKeeperContainerDetailModel>> result =
                        restTemplate.exchange(domain + "/api/keepercontainer/overload/info/all", HttpMethod.GET, null,
                                new ParameterizedTypeReference<List<MigrationKeeperContainerDetailModel>>() {});
                future().setSuccess(result.getBody());
            } catch (Throwable th) {
                getLogger().error("get migration keeper container detail:{} fail", domain, th);
                future().setFailure(th);
            }
        }

        @Override
        protected void doReset() {

        }
    }
}
