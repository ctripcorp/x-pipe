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
import com.ctrip.xpipe.utils.VisibleForTesting;
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

    private static final int DEFAULT_PEER_DATA_OVERLOAD = 474 * 1024 * 1024 * 1024;

    private static final int DEFAULT_KEEPER_FLOW_OVERLOAD = 270 * 1024;

    @Autowired
    private ConsoleConfig config;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    private Map<Date, Integer> checkerIndexes = new TreeMap<>();

    private Map<Integer, List<KeeperContainerUsedInfoModel>> allKeeperContainerUsedInfoModels = new HashMap<>();

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
    }

    @Override
    public synchronized void updateKeeperContainerUsedInfo(int index, List<KeeperContainerUsedInfoModel> keeperContainerUsedInfoModels) {
        if (keeperContainerUsedInfoModels != null && !keeperContainerUsedInfoModels.isEmpty()){
            allKeeperContainerUsedInfoModels.put(index, keeperContainerUsedInfoModels);
        }

        Date currentTime = new Date();
        checkerIndexes.put(currentTime, index);
        removeExpireData(currentTime);
        if (!checkDataIntegrity()) return;

        List<KeeperContainerUsedInfoModel> newKeeperContainerUsedInfoModels = new ArrayList<>();
        allKeeperContainerUsedInfoModels.values().forEach(newKeeperContainerUsedInfoModels::addAll);

        allKeeperContainerUsedInfoModels.clear();
        checkerIndexes.clear();

        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                analyzeKeeperContainerUsedInfo(newKeeperContainerUsedInfoModels);
            }
        });
    }

    private void removeExpireData(Date currentTime) {
        Iterator<Map.Entry<Date, Integer>> iterator = checkerIndexes.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Date, Integer> entry = iterator.next();
            logger.warn("[removeExpireData] remove expire index:{} time:{}, expire time:{}", entry.getValue(), entry.getKey(), config.getKeeperCheckerIntervalMilli());
            if (currentTime.getTime() - entry.getKey().getTime() > config.getKeeperCheckerIntervalMilli()) {
                allKeeperContainerUsedInfoModels.remove(entry.getValue());
                iterator.remove();
            }
            break;
        }
    }

    private boolean checkDataIntegrity() {
        return checkerIndexes.size() == config.getClusterDividedParts();
    }

    @VisibleForTesting
    void analyzeKeeperContainerUsedInfo(List<KeeperContainerUsedInfoModel> newKeeperContainerUsedInfoModels) {
        logger.debug("[analyzeKeeperContainerUsedInfo] newKeeperContainerUsedInfoModels: {}", newKeeperContainerUsedInfoModels);
        PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers = new PriorityQueue<>(newKeeperContainerUsedInfoModels.size(),
                (keeper1, keeper2) -> (int)(keeper1.getTotalInputFlow() - keeper2.getTotalInputFlow()));
        PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers = new PriorityQueue<>(newKeeperContainerUsedInfoModels.size(),
                (keeper1, keeper2) -> (int)(keeper1.getTotalRedisUsedMemory() - keeper2.getTotalRedisUsedMemory()));
        newKeeperContainerUsedInfoModels.forEach(keeperContainerInfoModel -> {
            minPeerDataKeeperContainers.add(keeperContainerInfoModel);
            minInputFlowKeeperContainers.add(keeperContainerInfoModel);
        });

        KeeperContainerOverloadStandardModel keeperContainerOverloadStandard = config.getKeeperContainerOverloadStandards().get(currentDc);
        if (keeperContainerOverloadStandard == null) {
            logger.warn("[analyzeKeeperContainerUsedInfo] keeperContainerOverloadStandard from dc {} is null," +
                    " use default config", currentDc);
            keeperContainerOverloadStandard = new KeeperContainerOverloadStandardModel()
                    .setFlowOverload(DEFAULT_KEEPER_FLOW_OVERLOAD).setPeerDataOverload(DEFAULT_PEER_DATA_OVERLOAD);
        }
        logger.debug("[analyzeKeeperContainerUsedInfo] keeperContainerOverloadStandard: {}", keeperContainerOverloadStandard);
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
            case PEER_DATA_OVERLOAD:
                return getMigrationKeeperDetails(src, true, overloadPeerData, keeperContainerOverloadStandard, minPeerDataKeeperContainers);
            case INPUT_FLOW_OVERLOAD:
            case BOTH:
                return getMigrationKeeperDetails(src, false, overloadInputFlow, keeperContainerOverloadStandard, minInputFlowKeeperContainers);
            default:
                logger.warn("invalid keeper container overload cause {}", overloadCause);
                return null;
        }
    }

    private List<MigrationKeeperContainerDetailModel> getMigrationKeeperDetails(KeeperContainerUsedInfoModel src,  boolean isPeerDataOverload,
                                           long overloadData, KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                                           PriorityQueue<KeeperContainerUsedInfoModel> availableKeeperContainers) {
        List<Map.Entry<DcClusterShard, Pair<Long, Long>>> allDcClusterShards = null;
        if (isPeerDataOverload) {
            allDcClusterShards = src.getDetailInfo().entrySet().stream()
                    .sorted((o1, o2) -> (int) (o2.getValue().getValue() - o1.getValue().getValue())).collect(Collectors.toList());
        } else {
            allDcClusterShards = src.getDetailInfo().entrySet().stream()
                    .sorted((o1, o2) -> (int) (o2.getValue().getKey() - o1.getValue().getKey())).collect(Collectors.toList());
        }

        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        KeeperContainerUsedInfoModel target = null;
        MigrationKeeperContainerDetailModel keeperContainerDetailModel = null;

        logger.debug("[analyzeKeeperContainerUsedInfo] src: {}, overlaodCause:{}, overloadData:{}, availableKeeperContainers:{} ",
                src, isPeerDataOverload, overloadData, availableKeeperContainers);

        for (Map.Entry<DcClusterShard, Pair<Long, Long>> dcClusterShard : allDcClusterShards) {
            if (target == null ) {
                target = availableKeeperContainers.poll();
                if (target == null) {
                    logger.warn("[analyzeKeeperContainerUsedInfo] no available keeper containers {} for overload keeper container {}",
                            availableKeeperContainers, src);
                    CatEventMonitor.DEFAULT.logEvent(KEEPER_RESOURCE_LACK, currentDc);
                    return result;
                }

                keeperContainerDetailModel = new MigrationKeeperContainerDetailModel(
                        new KeeperContainerUsedInfoModel(src.getKeeperIp(),src.getDcName(), src.getTotalInputFlow(), src.getTotalRedisUsedMemory()),
                        new KeeperContainerUsedInfoModel(target.getKeeperIp(),src.getDcName(), target.getTotalInputFlow(), target.getTotalRedisUsedMemory()),
                        0, new ArrayList<>());
            }

            long currentOverLoadData = isPeerDataOverload ? dcClusterShard.getValue().getValue() : dcClusterShard.getValue().getKey();

            long targetInputFlow = target.getTotalInputFlow() + dcClusterShard.getValue().getKey();
            long targetPeerData = target.getTotalRedisUsedMemory() + dcClusterShard.getValue().getValue();
            if (targetInputFlow > keeperContainerOverloadStandard.getFlowOverload()
                    || targetPeerData > keeperContainerOverloadStandard.getPeerDataOverload()) {
                logger.debug("[analyzeKeeperContainerUsedInfo] target keeper container {} can not add shard {}",
                        target, dcClusterShard);
                if (keeperContainerDetailModel.getMigrateKeeperCount() != 0) result.add(keeperContainerDetailModel);
                target = null;
                keeperContainerDetailModel = null;
                continue;
            }
            keeperContainerDetailModel.addReadyToMigrateShard(dcClusterShard.getKey());
            target.setTotalInputFlow(targetInputFlow).setTotalRedisUsedMemory(targetPeerData);

            if ((overloadData -= currentOverLoadData) <= 0) break;
        }

        if (keeperContainerDetailModel != null && keeperContainerDetailModel.getMigrateKeeperCount() != 0) {
            result.add(keeperContainerDetailModel);
        }

        if (target != null && target.getTotalInputFlow() < keeperContainerOverloadStandard.getPeerDataOverload()
                && target.getTotalRedisUsedMemory() < keeperContainerOverloadStandard.getPeerDataOverload()) {
            availableKeeperContainers.add(target);
        }

        return result;
    }

    private KeeperContainerOverloadCause getKeeperContainerOverloadCause(long overloadInputFlow, long overloadPeerData) {
        if (overloadInputFlow <= 0 && overloadPeerData <= 0) {
            return null;
        } else if (overloadInputFlow > 0 && overloadPeerData > 0) {
            return KeeperContainerOverloadCause.BOTH;
        } else if (overloadInputFlow > 0) {
            return KeeperContainerOverloadCause.INPUT_FLOW_OVERLOAD;
        } else {
            return KeeperContainerOverloadCause.PEER_DATA_OVERLOAD;
        }
    }

    @VisibleForTesting
    Map<Date, Integer> getCheckerIndexes() {
        return checkerIndexes;
    }

    @Override
    public Map<Integer, List<KeeperContainerUsedInfoModel>> getAllKeeperContainerUsedInfoModels() {
        return allKeeperContainerUsedInfoModels;
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
