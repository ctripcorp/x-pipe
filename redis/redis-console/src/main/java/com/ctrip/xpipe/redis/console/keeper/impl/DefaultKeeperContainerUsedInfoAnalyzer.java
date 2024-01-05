package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.Command.AbstractGetAllDcCommand;
import com.ctrip.xpipe.redis.console.keeper.Command.KeeperContainerFullSynchronizationTimeGetCommand;
import com.ctrip.xpipe.redis.console.keeper.Command.KeeperContainerInfoGetCommand;
import com.ctrip.xpipe.redis.console.keeper.Command.MigrationKeeperContainerDetailInfoGetCommand;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerOverloadCause;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.keeper.handler.KeeperContainerFilterChain;
import com.ctrip.xpipe.redis.console.keeper.entity.IPPairData;
import com.ctrip.xpipe.redis.console.keeper.util.DefaultKeeperContainerUsedInfoAnalyzerUtil;
import com.ctrip.xpipe.redis.console.keeper.util.KeeperContainerUsedInfoAnalyzerUtil;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.KeeperContainerAnalyzerService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class DefaultKeeperContainerUsedInfoAnalyzer extends AbstractService implements KeeperContainerUsedInfoAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerUsedInfoAnalyzer.class);

    private ConsoleConfig config;

    private KeeperContainerAnalyzerService keeperContainerAnalyzerService;

    private KeeperContainerFilterChain keeperContainerFilterChain;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    private Map<Integer, Pair<List<KeeperContainerUsedInfoModel>, Date>> keeperContainerUsedInfoModelIndexMap = new HashMap<>();

    private Map<String, KeeperContainerUsedInfoModel> currentDcAllKeeperContainerUsedInfoModelMap = new HashMap<>();

    private List<MigrationKeeperContainerDetailModel> currentDcKeeperContainerMigrationResult = new ArrayList<>();

    private long currentDcMaxKeeperContainerActiveRedisUsedMemory;

    private PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers;

    private PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers;

    private KeeperContainerUsedInfoAnalyzerUtil analyzerUtil = new DefaultKeeperContainerUsedInfoAnalyzerUtil();

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();

    private static final String KEEPER_RESOURCE_LACK = "keeper_resource_lack";

    public DefaultKeeperContainerUsedInfoAnalyzer() {}

    public DefaultKeeperContainerUsedInfoAnalyzer(ConsoleConfig config,
                                                  KeeperContainerAnalyzerService keeperContainerAnalyzerService,
                                                  KeeperContainerFilterChain keeperContainerFilterChain) {
        this.config = config;
        this.keeperContainerAnalyzerService = keeperContainerAnalyzerService;
        this.keeperContainerFilterChain = keeperContainerFilterChain;
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getAllDcReadyToMigrationKeeperContainers() {
        return getAllDcResult(this::getCurrentDcReadyToMigrationKeeperContainers,
                new MigrationKeeperContainerDetailInfoGetCommand(restTemplate),
                Collections.synchronizedList(new ArrayList<>()));
    }

    @Override
    public List<KeeperContainerUsedInfoModel> getAllDcKeeperContainerUsedInfoModelsList() {
        return getAllDcResult(this::getCurrentDcKeeperContainerUsedInfoModelsList,
                new KeeperContainerInfoGetCommand(restTemplate),
                Collections.synchronizedList(new ArrayList<>()));
    }

    @Override
    public List<Integer> getAllDcMaxKeeperContainerFullSynchronizationTime() {
        return getAllDcResult(this::getCurrentDcMaxKeeperContainerFullSynchronizationTime,
                new KeeperContainerFullSynchronizationTimeGetCommand(restTemplate),
                Collections.synchronizedList(new ArrayList<>()));
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getCurrentDcReadyToMigrationKeeperContainers() {
        return currentDcKeeperContainerMigrationResult;
    }

    @Override
    public List<KeeperContainerUsedInfoModel> getCurrentDcKeeperContainerUsedInfoModelsList() {
        return new ArrayList<>(currentDcAllKeeperContainerUsedInfoModelMap.values());
    }

    @Override
    public List<Integer> getCurrentDcMaxKeeperContainerFullSynchronizationTime() {
        List<Integer> result = new ArrayList<>();
        double keeperContainerIoRate = config.getKeeperContainerIoRate();
        if (!currentDcAllKeeperContainerUsedInfoModelMap.isEmpty()) {
            currentDcMaxKeeperContainerActiveRedisUsedMemory = analyzerUtil.getMaxActiveRedisUsedMemory(currentDcAllKeeperContainerUsedInfoModelMap);
        }
        result.add((int) (currentDcMaxKeeperContainerActiveRedisUsedMemory /1024/1024/keeperContainerIoRate/60));
        return result;
    }

    public <T> List<T> getAllDcResult(Supplier<List<T>> localDcResultSupplier, AbstractGetAllDcCommand<List<T>> command, List<T> result) {
        logger.info("[getAllDcResult] {} start", command.getName());
        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        config.getConsoleDomains().forEach((dc, domains) -> {
            if (currentDc.equalsIgnoreCase(dc)) {
                result.addAll(localDcResultSupplier.get());
            } else {
                AbstractGetAllDcCommand<List<T>> newCommand = command.clone();
                newCommand.setDomain(domains);
                newCommand.future().addListener(commandFuture -> {
                    if (commandFuture.isSuccess() && commandFuture.get() != null) {
                        logger.info("[getAllDcResult] getDc:{} Result success", dc);
                        result.addAll(commandFuture.get());
                    }
                });
                commandChain.add(newCommand);
            }
        });

        try {
            commandChain.execute().get(10, TimeUnit.SECONDS);
        } catch (Throwable th) {
            logger.warn("[getAllDcResult][{}] error:", command.getName(), th);
        }

        return result;
    }

    @Override
    public synchronized void updateKeeperContainerUsedInfo(int index, List<KeeperContainerUsedInfoModel> keeperContainerUsedInfoModels) {
        if (keeperContainerUsedInfoModels != null && !keeperContainerUsedInfoModels.isEmpty()){
            keeperContainerUsedInfoModelIndexMap.put(index, new Pair<>(keeperContainerUsedInfoModels, new Date()));
        }
        removeExpireData();
        logger.info("[analyzeKeeperContainerUsedInfo] current index {}", keeperContainerUsedInfoModelIndexMap.keySet());
        if (keeperContainerUsedInfoModelIndexMap.size() != config.getClusterDividedParts()) return;

        currentDcAllKeeperContainerUsedInfoModelMap.clear();
        keeperContainerUsedInfoModelIndexMap.values().forEach(list -> list.getKey().forEach(infoModel -> currentDcAllKeeperContainerUsedInfoModelMap.put(infoModel.getKeeperIp(), infoModel)));

        keeperContainerUsedInfoModelIndexMap.clear();
        logger.info("[analyzeKeeperContainerUsedInfo] start analyze allKeeperContainerUsedInfoModelsList");
        if (currentDcAllKeeperContainerUsedInfoModelMap.isEmpty()) return;
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                TransactionMonitor transaction = TransactionMonitor.DEFAULT;
                transaction.logTransactionSwallowException("keeperContainer.analyze", currentDc, new Task() {
                    @Override
                    public void go() throws Exception {
                        analyzeKeeperContainerUsedInfo();
                    }

                    @Override
                    public Map<String, Object> getData() {
                        Map<String, Object> transactionData = new HashMap<>();
                        transactionData.put("keeperContainerSize", currentDcAllKeeperContainerUsedInfoModelMap.size());
                        return transactionData;
                    }
                });
            }
        });
    }

    private void removeExpireData() {
        List<Integer> expireIndex = new ArrayList<>();
        for (Map.Entry<Integer, Pair<List<KeeperContainerUsedInfoModel>, Date>> entry : keeperContainerUsedInfoModelIndexMap.entrySet()) {
            if (new Date().getTime() - entry.getValue().getValue().getTime() > config.getKeeperCheckerIntervalMilli() * 2L) {
                expireIndex.add(entry.getKey());
            }
        }
        for (int index : expireIndex) {
            logger.info("[removeExpireData] remove expire index:{} time:{}, expire time:{}", index, keeperContainerUsedInfoModelIndexMap.get(index).getValue(), config.getKeeperCheckerIntervalMilli() * 2L);
            keeperContainerUsedInfoModelIndexMap.remove(index);
        }
    }

    @VisibleForTesting
    void analyzeKeeperContainerUsedInfo() {
        logger.info("[analyzeKeeperContainerUsedInfo] start, keeperContainer number {}", currentDcAllKeeperContainerUsedInfoModelMap.size());
        analyzerUtil.initKeeperPairData(currentDcAllKeeperContainerUsedInfoModelMap);
        keeperContainerAnalyzerService.initStandard(currentDcAllKeeperContainerUsedInfoModelMap);
        generateAllSortedDescKeeperContainerUsedInfoModelQueue();
        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        for (KeeperContainerUsedInfoModel infoModel : currentDcAllKeeperContainerUsedInfoModelMap.values()) {
            List<MigrationKeeperContainerDetailModel> migrationKeeperDetails = getOverloadKeeperMigrationDetails(infoModel);
            if (migrationKeeperDetails != null)
                result.addAll(migrationKeeperDetails);
            for (String ip : analyzerUtil.getAllPairsIP(infoModel.getKeeperIp())) {
                List<MigrationKeeperContainerDetailModel> keeperPairMigrationKeeperDetails = getKeeperPairMigrationKeeperDetails(infoModel, currentDcAllKeeperContainerUsedInfoModelMap.get(ip));
                if (keeperPairMigrationKeeperDetails != null)
                    result.addAll(keeperPairMigrationKeeperDetails);
            }
        }
        currentDcKeeperContainerMigrationResult = result;
    }

    private void generateAllSortedDescKeeperContainerUsedInfoModelQueue() {
        minInputFlowKeeperContainers = new PriorityQueue<>(currentDcAllKeeperContainerUsedInfoModelMap.values().size(),
                (keeper1, keeper2) -> (int)(keeper1.getActiveInputFlow() - keeper2.getActiveInputFlow()));
        minPeerDataKeeperContainers = new PriorityQueue<>(currentDcAllKeeperContainerUsedInfoModelMap.values().size(),
                (keeper1, keeper2) -> (int)(keeper1.getTotalRedisUsedMemory() - keeper2.getTotalRedisUsedMemory()));
        currentDcAllKeeperContainerUsedInfoModelMap.values().forEach(keeperContainerInfoModel -> {
            if (keeperContainerFilterChain.doKeeperContainerFilter(keeperContainerInfoModel)) {
                minPeerDataKeeperContainers.add(keeperContainerInfoModel);
                minInputFlowKeeperContainers.add(keeperContainerInfoModel);
            }
        });
    }


    private List<MigrationKeeperContainerDetailModel> getOverloadKeeperMigrationDetails(KeeperContainerUsedInfoModel infoModel) {
        long overloadInputFlow = infoModel.getActiveInputFlow() - infoModel.getInputFlowStandard();
        long overloadPeerData = infoModel.getActiveRedisUsedMemory() - infoModel.getRedisUsedMemoryStandard();
        KeeperContainerOverloadCause overloadCause = getKeeperContainerOverloadCause(overloadInputFlow, overloadPeerData);
        if (overloadCause == null) return null;

        switch (overloadCause) {
            case PEER_DATA_OVERLOAD:
                return getOverloadKeeperMigrationDetails(infoModel, true, overloadPeerData, minPeerDataKeeperContainers, overloadCause);
            case INPUT_FLOW_OVERLOAD:
            case BOTH:
                return getOverloadKeeperMigrationDetails(infoModel, false, overloadInputFlow, minInputFlowKeeperContainers, overloadCause);
            default:
                logger.warn("invalid keeper container overload cause {}", overloadCause);
                return null;
        }
    }

    private List<MigrationKeeperContainerDetailModel> getOverloadKeeperMigrationDetails(KeeperContainerUsedInfoModel src,
                                                                                        boolean isPeerDataOverload,
                                                                                        long overloadData,
                                                                                        PriorityQueue<KeeperContainerUsedInfoModel> availableKeeperContainers,
                                                                                        KeeperContainerOverloadCause overloadCause) {
        logger.info("[analyzeKeeperContainerUsedInfo] srcIp: {}, overloadCause:{}, overloadData:{}", src.getKeeperIp(), overloadCause.name(), overloadData);
        PriorityQueue<KeeperContainerUsedInfoModel> anotherQueue = isPeerDataOverload ? minInputFlowKeeperContainers : minPeerDataKeeperContainers;
        List<Map.Entry<DcClusterShardActive, KeeperUsedInfo>> allDescDcClusterShards = getDescDcClusterShardDetails(src.getDetailInfo(), isPeerDataOverload);
        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        KeeperContainerUsedInfoModel target = null;
        MigrationKeeperContainerDetailModel keeperContainerMigrationDetail = null;
        MigrationKeeperContainerDetailModel switchActiveMigrationDetail = new MigrationKeeperContainerDetailModel(src, null, 0, true, false, overloadCause.name(), new ArrayList<>());
        for (Map.Entry<DcClusterShardActive, KeeperUsedInfo> dcClusterShard : allDescDcClusterShards) {
            if (overloadData <= 0) break;
            if (!dcClusterShard.getKey().isActive()) continue;
            String backUpKeeperIP = analyzerUtil.getBackUpKeeperIp(dcClusterShard.getKey());
            KeeperContainerUsedInfoModel backUpKeeper = currentDcAllKeeperContainerUsedInfoModelMap.get(backUpKeeperIP);
            if (keeperContainerFilterChain.doKeeperContainerFilter(backUpKeeper) && keeperContainerFilterChain.doKeeperFilter(dcClusterShard, src, backUpKeeper, analyzerUtil)) {
                switchActiveMigrationDetail.addReadyToMigrateShard(dcClusterShard.getKey());
                updateAllSortedDescKeeperContainerUsedInfoModelQueue(backUpKeeper.getKeeperIp(), new KeeperContainerUsedInfoModel(backUpKeeper, dcClusterShard));
                if (target != null && backUpKeeper.getKeeperIp().equals(target.getKeeperIp())) {
                    target = new KeeperContainerUsedInfoModel(backUpKeeper, dcClusterShard);
                }
                overloadData = updateOverLoadData(isPeerDataOverload, overloadData, dcClusterShard.getValue());
                continue;
            }
            if (target == null ) {
                target = availableKeeperContainers.poll();
                if (target == null) {
                    logger.warn("[analyzeKeeperContainerUsedInfo] no available keeper containers {} for overload keeper container {}", availableKeeperContainers, src);
                    CatEventMonitor.DEFAULT.logEvent(KEEPER_RESOURCE_LACK, currentDc);
                    generateResult(result, keeperContainerMigrationDetail, switchActiveMigrationDetail);
                    return result;
                }
                keeperContainerMigrationDetail = new MigrationKeeperContainerDetailModel(src, target, 0, false, false, overloadCause.name(), new ArrayList<>());
            }
            if (!keeperContainerFilterChain.doKeeperFilter(dcClusterShard, src, target, analyzerUtil) ||
                    !keeperContainerFilterChain.doKeeperPairFilter(dcClusterShard, backUpKeeper, target, analyzerUtil)) {
                updateSortedDescKeeperContainerUsedInfoModelQueue(anotherQueue, target.getKeeperIp(), target);
                target = null;
                if (keeperContainerMigrationDetail.getMigrateKeeperCount() != 0) result.add(keeperContainerMigrationDetail);
                continue;
            }
            keeperContainerMigrationDetail.addReadyToMigrateShard(dcClusterShard.getKey());
            target.setActiveInputFlow(target.getActiveInputFlow() + dcClusterShard.getValue().getInputFlow()).setTotalRedisUsedMemory(target.getTotalRedisUsedMemory() + dcClusterShard.getValue().getPeerData());
            analyzerUtil.updateMigrateIpPair(src.getKeeperIp(), backUpKeeper.getKeeperIp(), target.getKeeperIp(), dcClusterShard);
            overloadData = updateOverLoadData(isPeerDataOverload, overloadData, dcClusterShard.getValue());
        }

        generateResult(result, keeperContainerMigrationDetail, switchActiveMigrationDetail);

        if (target != null && target.getActiveInputFlow() < src.getInputFlowStandard() && target.getTotalRedisUsedMemory() < src.getRedisUsedMemoryStandard()) {
            availableKeeperContainers.add(target);
            updateSortedDescKeeperContainerUsedInfoModelQueue(anotherQueue, target.getKeeperIp(), target);
        }
        return result;
    }

    private List<MigrationKeeperContainerDetailModel> getKeeperPairMigrationKeeperDetails(KeeperContainerUsedInfoModel pairA, KeeperContainerUsedInfoModel pairB){
        double keeperPairOverLoadFactor = config.getKeeperPairOverLoadFactor();
        KeeperContainerOverloadStandardModel minStandardModel = new KeeperContainerOverloadStandardModel()
                .setFlowOverload((long) (Math.min(pairB.getInputFlowStandard(), pairA.getInputFlowStandard()) * keeperPairOverLoadFactor))
                .setPeerDataOverload((long) (Math.min(pairB.getRedisUsedMemoryStandard(), pairA.getRedisUsedMemoryStandard()) * keeperPairOverLoadFactor));

        IPPairData longLongPair = analyzerUtil.getIPPairData(pairA.getKeeperIp(), pairB.getKeeperIp());
        long overloadInputFlow = longLongPair.getInputFlow() - minStandardModel.getFlowOverload();
        long overloadPeerData = longLongPair.getPeerData() - minStandardModel.getPeerDataOverload();
        KeeperContainerOverloadCause overloadCause = getKeeperPairOverloadCause(overloadInputFlow, overloadPeerData);
        if (overloadCause == null) return null;

        switch (overloadCause) {
            case KEEPER_PAIR_PEER_DATA_OVERLOAD:
                return getKeeperPairMigrationDetails(pairA, pairB, true, overloadPeerData, minPeerDataKeeperContainers, overloadCause);
            case KEEPER_PAIR_INPUT_FLOW_OVERLOAD:
            case KEEPER_PAIR_BOTH:
                return getKeeperPairMigrationDetails(pairA, pairB, false, overloadInputFlow, minInputFlowKeeperContainers, overloadCause);
            default:
                logger.warn("invalid keeper container overload cause {}", overloadCause);
                return null;
        }

    }

    private KeeperContainerOverloadCause getKeeperPairOverloadCause(long overloadInputFlow, long overloadPeerData) {
        if (overloadInputFlow <= 0 && overloadPeerData <= 0) {
            return null;
        } else if (overloadInputFlow > 0 && overloadPeerData > 0) {
            return KeeperContainerOverloadCause.KEEPER_PAIR_BOTH;
        } else if (overloadInputFlow > 0) {
            return KeeperContainerOverloadCause.KEEPER_PAIR_INPUT_FLOW_OVERLOAD;
        } else {
            return KeeperContainerOverloadCause.KEEPER_PAIR_PEER_DATA_OVERLOAD;
        }
    }

    private List<MigrationKeeperContainerDetailModel> getKeeperPairMigrationDetails(KeeperContainerUsedInfoModel pairA,
                                                                                    KeeperContainerUsedInfoModel pairB,
                                                                                    boolean isPeerDataOverload,
                                                                                    long overloadData,
                                                                                    PriorityQueue<KeeperContainerUsedInfoModel> availableKeeperContainers,
                                                                                    KeeperContainerOverloadCause overloadCause){
        List<Map.Entry<DcClusterShardActive, KeeperUsedInfo>> allDcClusterShards = getDescDcClusterShardDetails(analyzerUtil.getAllDetailInfo(pairA.getKeeperIp(), pairB.getKeeperIp()), isPeerDataOverload);
        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        KeeperContainerUsedInfoModel target = null;
        List<KeeperContainerUsedInfoModel> usedTarget = new ArrayList<>();
        MigrationKeeperContainerDetailModel keeperContainerDetailModel = null;
        logger.debug("[analyzeKeeperPairOverLoad] pairA: {}, pairB: {}, overloadCause:{}, overloadData:{}, availableKeeperContainers:{} ", pairA, pairB, isPeerDataOverload, overloadData, availableKeeperContainers);
        for (Map.Entry<DcClusterShardActive, KeeperUsedInfo> dcClusterShard : allDcClusterShards) {
            if (overloadData <= 0) break;
            if (!dcClusterShard.getKey().isActive()) continue;
            String srcKeeperIp = dcClusterShard.getValue().getKeeperIP();
            KeeperContainerUsedInfoModel srcKeeperContainer = srcKeeperIp.equals(pairA.getKeeperIp()) ? pairA : pairB;
            KeeperContainerUsedInfoModel backUpKeeperContainer = srcKeeperIp.equals(pairA.getKeeperIp()) ? pairB : pairA;
            while (target == null) {
                target = availableKeeperContainers.poll();
                if (target == null) {
                    logger.warn("[analyzeKeeperContainerUsedInfo] no available keeper containers {} for overload keeper pair {},{}", availableKeeperContainers, pairA, pairB);
                    CatEventMonitor.DEFAULT.logEvent(KEEPER_RESOURCE_LACK, currentDc);
                    availableKeeperContainers.addAll(usedTarget);
                    return result;
                }
                if (target.getKeeperIp().equals(pairA.getKeeperIp()) || target.getKeeperIp().equals(pairB.getKeeperIp())) {
                    usedTarget.add(target);
                    target = availableKeeperContainers.poll();
                }
                keeperContainerDetailModel = new MigrationKeeperContainerDetailModel(srcKeeperContainer, target, 0, false, true, overloadCause.name(), new ArrayList<>());
            }
            if (!keeperContainerFilterChain.doKeeperPairFilter(dcClusterShard, srcKeeperContainer, target, analyzerUtil)) {
                usedTarget.add(target);
                target = null;
                generateResult(result, keeperContainerDetailModel);
                continue;
            }
            keeperContainerDetailModel.addReadyToMigrateShard(dcClusterShard.getKey());
            analyzerUtil.updateMigrateIpPair(srcKeeperContainer.getKeeperIp(), backUpKeeperContainer.getKeeperIp(), target.getKeeperIp(), dcClusterShard);
            overloadData = updateOverLoadData(isPeerDataOverload, overloadData, dcClusterShard.getValue());
        }

        if (target != null) {
            usedTarget.add(target);
        }

        generateResult(result, keeperContainerDetailModel);
        availableKeeperContainers.addAll(usedTarget);

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

    private List<Map.Entry<DcClusterShardActive, KeeperUsedInfo>> getDescDcClusterShardDetails(Map<DcClusterShardActive, KeeperUsedInfo> allDetailInfo, boolean isPeerDataOverload) {
        Comparator<Map.Entry<DcClusterShardActive, KeeperUsedInfo>> comparator = isPeerDataOverload ?
                Comparator.comparingLong(e -> e.getValue().getPeerData()) : Comparator.comparingLong(e -> e.getValue().getInputFlow());
        return allDetailInfo.entrySet().stream()
                .sorted(comparator.reversed())
                .collect(Collectors.toList());
    }

    private void updateAllSortedDescKeeperContainerUsedInfoModelQueue(String keeperIp, KeeperContainerUsedInfoModel newModel) {
        updateSortedDescKeeperContainerUsedInfoModelQueue(minInputFlowKeeperContainers, keeperIp, newModel);
        updateSortedDescKeeperContainerUsedInfoModelQueue(minPeerDataKeeperContainers, keeperIp, newModel);
    }

    private void updateSortedDescKeeperContainerUsedInfoModelQueue(PriorityQueue<KeeperContainerUsedInfoModel> queue, String keeperIp, KeeperContainerUsedInfoModel newModel) {
        Queue<KeeperContainerUsedInfoModel> temp = new LinkedList<>();
        while (!queue.isEmpty()){
            KeeperContainerUsedInfoModel infoModel = queue.poll();
            if (infoModel.getKeeperIp().equals(keeperIp)) {
                temp.add(newModel);
                break;
            }
            temp.add(infoModel);
        }
        while(!temp.isEmpty()) {
            queue.add(temp.poll());
        }
    }

    private long updateOverLoadData(boolean isPeerDataOverload, long overloadData, KeeperUsedInfo usedInfo) {
        long currentOverLoadData = isPeerDataOverload ? usedInfo.getPeerData() : usedInfo.getInputFlow();
        return overloadData - currentOverLoadData;
    }

    private void generateResult(List<MigrationKeeperContainerDetailModel> result, MigrationKeeperContainerDetailModel... detailModel) {
        for (MigrationKeeperContainerDetailModel model : detailModel) {
            if (model != null && model.getMigrateKeeperCount() != 0) {
                result.add(model);
            }
        }
    }

    @VisibleForTesting
    int getCheckerIndexesSize() {
        return keeperContainerUsedInfoModelIndexMap.size();
    }

    @VisibleForTesting
    void setKeeperContainerAnalyzerService(KeeperContainerAnalyzerService keeperContainerAnalyzerService) {
        this.keeperContainerAnalyzerService = keeperContainerAnalyzerService;
    }

    @VisibleForTesting
    void setExecutors(Executor executors){
        this.executors = executors;
    }

    @VisibleForTesting
    void setKeeperContainerFilterChain(KeeperContainerFilterChain keeperContainerFilterChain){
        this.keeperContainerFilterChain = keeperContainerFilterChain;
    }

    @VisibleForTesting
    Map<String, KeeperContainerUsedInfoModel> getCurrentDcKeeperContainerUsedInfoModelsMap() {
        return currentDcAllKeeperContainerUsedInfoModelMap;
    }

}
