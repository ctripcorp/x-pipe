package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerOverloadCause;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.keeper.handler.KeeperContainerFilterChain;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class DefaultKeeperContainerUsedInfoAnalyzer extends AbstractService implements KeeperContainerUsedInfoAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerUsedInfoAnalyzer.class);

    private static final long DEFAULT_PEER_DATA_OVERLOAD = 474L * 1024 * 1024 * 1024;

    private static final long DEFAULT_KEEPER_FLOW_OVERLOAD = 270 * 1024;

    private ConsoleConfig config;

    private KeeperContainerService keeperContainerService;

    private KeeperContainerFilterChain keeperContainerFilterChain;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    private Map<Integer, Date> checkerIndexes = new HashMap<>();

    private Map<Integer, List<KeeperContainerUsedInfoModel>> keeperContainerUsedInfoModelIndexMap = new HashMap<>();

    private Map<String, KeeperContainerUsedInfoModel> currentDcAllKeeperContainerUsedInfoModelMap = new HashMap<>();

    private List<MigrationKeeperContainerDetailModel> currentDcKeeperContainerMigrationDetail = new ArrayList<>();

    private long currentDcMaxKeeperContainerActiveRedisUsedMemory;

    private Map<DcClusterShardActive, KeeperUsedInfo> allDetailInfo = new HashMap<>();

    private PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers;

    private PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers;

    Map<IPPair, IPPairData> keeperPairUsedInfoMap = new HashMap<>();

    Map<String, Map<String ,Map<DcClusterShardActive, KeeperUsedInfo>>> ipPairMap = new HashMap<>();

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    private static final String KEEPER_RESOURCE_LACK = "keeper_resource_lack";

    public DefaultKeeperContainerUsedInfoAnalyzer() {}

    public DefaultKeeperContainerUsedInfoAnalyzer(ConsoleConfig config,
                                                  KeeperContainerService keeperContainerService,
                                                  KeeperContainerFilterChain keeperContainerFilterChain) {
        this.config = config;
        this.keeperContainerService = keeperContainerService;
        this.keeperContainerFilterChain = keeperContainerFilterChain;
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getAllDcReadyToMigrationKeeperContainers() {
        List<MigrationKeeperContainerDetailModel> result = Collections.synchronizedList(new ArrayList<>());
        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        config.getConsoleDomains().forEach((dc, domains) -> {
            if (currentDc.equalsIgnoreCase(dc)) {
                result.addAll(getCurrentDcReadyToMigrationKeeperContainers());
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
    public List<MigrationKeeperContainerDetailModel> getCurrentDcReadyToMigrationKeeperContainers() {
        return currentDcKeeperContainerMigrationDetail;
    }

    @Override
    public List<KeeperContainerUsedInfoModel> getAllDcKeeperContainerUsedInfoModelsList() {
        List<KeeperContainerUsedInfoModel> result = Collections.synchronizedList(new ArrayList<>());
        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        config.getConsoleDomains().forEach((dc, domains) -> {
            if (currentDc.equalsIgnoreCase(dc)) {
                result.addAll(getCurrentDcKeeperContainerUsedInfoModelsList().values());
            } else {
                KeeperContainerInfoGetCommand command = new KeeperContainerInfoGetCommand(domains, restTemplate);
                command.future().addListener(commandFuture -> {
                    if (commandFuture.isSuccess() && commandFuture.get() != null) result.addAll(commandFuture.get());
                });
                commandChain.add(command);
            }
        });

        try {
            commandChain.execute().get(10, TimeUnit.SECONDS);
        } catch (Throwable th) {
            logger.warn("[getAllKeeperContainerUsedInfoModelsList] error:", th);
        }

        return result;
    }

    @Override
    public Map<String, KeeperContainerUsedInfoModel> getCurrentDcKeeperContainerUsedInfoModelsList() {
        return currentDcAllKeeperContainerUsedInfoModelMap;
    }

    @Override
    public int getAllDcMaxKeeperContainerFullSynchronizationTime() {
        AtomicInteger result = new AtomicInteger();
        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        config.getConsoleDomains().forEach((dc, domains) -> {
            if (currentDc.equalsIgnoreCase(dc)) {
                result.set(Math.max(result.get(), getCurrentDcMaxKeeperContainerFullSynchronizationTime()));
            } else {
                KeeperContainerFullSynchronizationTimeGetCommand command = new KeeperContainerFullSynchronizationTimeGetCommand(domains, restTemplate);
                command.future().addListener(commandFuture -> {
                    if (commandFuture.isSuccess() && commandFuture.get() != null) result.set(Math.max(result.get(), commandFuture.get()));
                });
                commandChain.add(command);
            }
        });

        try {
            commandChain.execute().get(10, TimeUnit.SECONDS);
        } catch (Throwable th) {
            logger.warn("[getMaxKeeperContainerFullSynchronizationTime] error:", th);
        }

        return result.get();
    }

    @Override
    public int getCurrentDcMaxKeeperContainerFullSynchronizationTime() {
        double keeperContainerIoRate = config.getKeeperContainerIoRate();
        return (int) (currentDcMaxKeeperContainerActiveRedisUsedMemory /1024/1024/keeperContainerIoRate/60);
    }

    @Override
    public synchronized void updateKeeperContainerUsedInfo(int index, List<KeeperContainerUsedInfoModel> keeperContainerUsedInfoModels) {
        if (keeperContainerUsedInfoModels != null && !keeperContainerUsedInfoModels.isEmpty()){
            keeperContainerUsedInfoModelIndexMap.put(index, keeperContainerUsedInfoModels);
        }

        Date currentTime = new Date();
        checkerIndexes.put(index, currentTime);
        removeExpireData(currentTime);
        if (!checkDataIntegrity()) return;

        currentDcAllKeeperContainerUsedInfoModelMap.clear();
        keeperContainerUsedInfoModelIndexMap.values().forEach(list -> list.forEach(infoModel -> currentDcAllKeeperContainerUsedInfoModelMap.put(infoModel.getKeeperIp(), infoModel)));

        keeperContainerUsedInfoModelIndexMap.clear();
        checkerIndexes.clear();
        logger.info("[analyzeKeeperContainerUsedInfo] start analyze allKeeperContainerUsedInfoModelsList {}", currentDcAllKeeperContainerUsedInfoModelMap);
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                analyzeKeeperContainerUsedInfo();
            }
        });
    }

    private void removeExpireData(Date currentTime) {
        List<Integer> expireIndex = new ArrayList<>();
        for (Map.Entry<Integer, Date> entry : checkerIndexes.entrySet()) {
            if (currentTime.getTime() - entry.getValue().getTime() > config.getKeeperCheckerIntervalMilli()) {
                expireIndex.add(entry.getKey());
            }
        }
        for (int index : expireIndex) {
            logger.info("[removeExpireData] remove expire index:{} time:{}, expire time:{}", index, checkerIndexes.get(index), config.getKeeperCheckerIntervalMilli());
            keeperContainerUsedInfoModelIndexMap.remove(index);
            checkerIndexes.remove(index);
        }
    }

    private boolean checkDataIntegrity() {
        logger.info("[analyzeKeeperContainerUsedInfo] current index {}", checkerIndexes);
        return checkerIndexes.size() == config.getClusterDividedParts();
    }

    @VisibleForTesting
    void analyzeKeeperContainerUsedInfo() {
        analyzeKeeperPair();
        analyzeKeeperContainerStandard();
        generateAllSortedDescKeeperContainerUsedInfoModelQueue();
        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        for (KeeperContainerUsedInfoModel infoModel : currentDcAllKeeperContainerUsedInfoModelMap.values()) {
            List<MigrationKeeperContainerDetailModel> migrationKeeperDetails = getOverloadKeeperMigrationDetails(infoModel);
            if (migrationKeeperDetails != null)
                result.addAll(migrationKeeperDetails);
            for (String ip : getAllIPPairs(infoModel.getKeeperIp())) {
                List<MigrationKeeperContainerDetailModel> keeperPairMigrationKeeperDetails = getKeeperPairMigrationKeeperDetails(infoModel, currentDcAllKeeperContainerUsedInfoModelMap.get(ip));
                if (keeperPairMigrationKeeperDetails != null)
                    result.addAll(keeperPairMigrationKeeperDetails);
            }
        }
        currentDcKeeperContainerMigrationDetail = result;
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

    private void analyzeKeeperPair(){
        keeperPairUsedInfoMap.clear();
        ipPairMap.clear();
        allDetailInfo.clear();
        currentDcMaxKeeperContainerActiveRedisUsedMemory = 0;
        for (KeeperContainerUsedInfoModel infoModel : currentDcAllKeeperContainerUsedInfoModelMap.values()) {
            if (infoModel.getActiveRedisUsedMemory() > currentDcMaxKeeperContainerActiveRedisUsedMemory) {
                currentDcMaxKeeperContainerActiveRedisUsedMemory = infoModel.getActiveRedisUsedMemory();
            }
            if (infoModel.getDetailInfo() != null) {
                allDetailInfo.putAll(infoModel.getDetailInfo());
            }
        }
        for (Map.Entry<DcClusterShardActive, KeeperUsedInfo> entry : allDetailInfo.entrySet()) {
            if (!entry.getKey().isActive()) continue;
            KeeperUsedInfo activeKeeperUsedInfo = entry.getValue();
            KeeperUsedInfo backUpKeeperUsedInfo = allDetailInfo.get(new DcClusterShardActive(entry.getKey(), false));
            if (backUpKeeperUsedInfo == null) {
                logger.warn("[analyzeKeeperPair] active keeper {} has no backup keeper", entry.getKey());
                continue;
            }
            IPPair ipPair = new IPPair(activeKeeperUsedInfo.getKeeperIP(), backUpKeeperUsedInfo.getKeeperIP());
            IPPairData ipPairData = new IPPairData(activeKeeperUsedInfo.getInputFlow(), activeKeeperUsedInfo.getPeerData(), 1);
            if (keeperPairUsedInfoMap.containsKey(ipPair)) {
                IPPairData value = keeperPairUsedInfoMap.get(ipPair);
                ipPairData = new IPPairData(activeKeeperUsedInfo.getInputFlow() + value.getInputFlow(), activeKeeperUsedInfo.getPeerData() + value.getPeerData(), value.getNumber() + 1);
            }
            keeperPairUsedInfoMap.put(ipPair, ipPairData);

            generateIpPairMap(entry, activeKeeperUsedInfo, backUpKeeperUsedInfo);
            generateIpPairMap(entry, backUpKeeperUsedInfo, activeKeeperUsedInfo);
        }
    }

    private void generateIpPairMap(Map.Entry<DcClusterShardActive, KeeperUsedInfo> entry, KeeperUsedInfo keeper1, KeeperUsedInfo keeper2) {
        if (!ipPairMap.containsKey(keeper1.getKeeperIP())) {
            ipPairMap.put(keeper1.getKeeperIP(), new HashMap<>());
        }
        if (!ipPairMap.get(keeper1.getKeeperIP()).containsKey(keeper2.getKeeperIP())) {
            ipPairMap.get(keeper1.getKeeperIP()).put(keeper2.getKeeperIP(), new HashMap<>());
        }
        ipPairMap.get(keeper1.getKeeperIP()).get(keeper2.getKeeperIP()).put(entry.getKey(), entry.getValue());
    }

    private List<String> getAllIPPairs(String ip) {
        List<String> ipPairs = new ArrayList<>();
        if (ipPairMap.containsKey(ip)) {
            ipPairs.addAll(ipPairMap.get(ip).keySet());
        }
        return ipPairs;
    }

    @VisibleForTesting
    void analyzeKeeperContainerStandard() {
        KeeperContainerOverloadStandardModel keeperContainerOverloadStandard = config.getKeeperContainerOverloadStandards().get(currentDc);
        double loadFactor = config.getKeeperContainerOverloadFactor();
        KeeperContainerOverloadStandardModel defaultOverloadStandard = getDefaultStandard(keeperContainerOverloadStandard, loadFactor);
        logger.info("[analyzeKeeperContainerUsedInfo] keeperContainerDefaultOverloadStandard: {}", defaultOverloadStandard);
        for (KeeperContainerUsedInfoModel infoModel : currentDcAllKeeperContainerUsedInfoModelMap.values()) {
            KeeperContainerOverloadStandardModel realKeeperContainerOverloadStandard = getRealStandard(keeperContainerOverloadStandard, defaultOverloadStandard, infoModel, loadFactor);
            infoModel.setInputFlowStandard(realKeeperContainerOverloadStandard.getFlowOverload());
            infoModel.setRedisUsedMemoryStandard(realKeeperContainerOverloadStandard.getPeerDataOverload());
        }
    }

    private KeeperContainerOverloadStandardModel getDefaultStandard(KeeperContainerOverloadStandardModel keeperContainerOverloadStandard, double loadFactor){
        if (keeperContainerOverloadStandard == null) {
            return new KeeperContainerOverloadStandardModel()
                    .setFlowOverload((long) (DEFAULT_KEEPER_FLOW_OVERLOAD * loadFactor)).setPeerDataOverload((long) (DEFAULT_PEER_DATA_OVERLOAD * loadFactor));
        } else {
            return new KeeperContainerOverloadStandardModel()
                    .setFlowOverload((long) (keeperContainerOverloadStandard.getFlowOverload() * loadFactor)).setPeerDataOverload((long) (keeperContainerOverloadStandard.getFlowOverload() * loadFactor));
        }
    }

    private KeeperContainerOverloadStandardModel getRealStandard(KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                                                                 KeeperContainerOverloadStandardModel defaultOverloadStandard,
                                                                 KeeperContainerUsedInfoModel infoModel,
                                                                 double loadFactor){
        KeepercontainerTbl keepercontainerTbl = keeperContainerService.find(infoModel.getKeeperIp());
        infoModel.setDiskType(keepercontainerTbl.getKeepercontainerDiskType());
        infoModel.setKeeperContainerActive(keepercontainerTbl.isKeepercontainerActive());
        if (keeperContainerOverloadStandard != null && keeperContainerOverloadStandard.getDiskTypes() != null && !keeperContainerOverloadStandard.getDiskTypes().isEmpty()) {
            for (KeeperContainerOverloadStandardModel.DiskTypesEnum diskType : keeperContainerOverloadStandard.getDiskTypes()) {
                if (diskType.getDiskType().getDesc().equals(keepercontainerTbl.getKeepercontainerDiskType())) {
                    return new KeeperContainerOverloadStandardModel()
                                    .setFlowOverload((long) (diskType.getFlowOverload() * loadFactor))
                                    .setPeerDataOverload((long) (diskType.getPeerDataOverload() * loadFactor));

                }
            }
        } else {
            logger.warn("[analyzeKeeperContainerUsedInfo] keeperContainerOverloadStandard diskType {} from dc {} is null," +
                    " use default config", keepercontainerTbl.getKeepercontainerDiskType() ,currentDc);
        }
        return defaultOverloadStandard;
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
        PriorityQueue<KeeperContainerUsedInfoModel> anotherQueue = isPeerDataOverload ? minInputFlowKeeperContainers : minPeerDataKeeperContainers;
        List<Map.Entry<DcClusterShardActive, KeeperUsedInfo>> allDescDcClusterShards = getDescDcClusterShardDetails(src.getDetailInfo(), isPeerDataOverload);
        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        KeeperContainerUsedInfoModel target = null;
        MigrationKeeperContainerDetailModel keeperContainerMigrationDetail = null;
        MigrationKeeperContainerDetailModel switchActiveMigrationDetail = new MigrationKeeperContainerDetailModel(src, null, 0, true, false, overloadCause.name(), new ArrayList<>());
        logger.info("[analyzeKeeperContainerUsedInfo] src: {}, overloadCause:{}, overloadData:{}, availableKeeperContainers:{} ", src, isPeerDataOverload, overloadData, availableKeeperContainers);
        for (Map.Entry<DcClusterShardActive, KeeperUsedInfo> dcClusterShard : allDescDcClusterShards) {
            if (overloadData <= 0) break;
            if (!dcClusterShard.getKey().isActive()) continue;
            KeeperContainerUsedInfoModel backUpKeeper = getBackUpKeeper(dcClusterShard.getKey());
            if (keeperContainerFilterChain.doKeeperContainerFilter(backUpKeeper) && keeperContainerFilterChain.doKeeperFilter(dcClusterShard, src, backUpKeeper, keeperPairUsedInfoMap)) {
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
            if (!keeperContainerFilterChain.doKeeperFilter(dcClusterShard, src, target, keeperPairUsedInfoMap) ||
                    !keeperContainerFilterChain.doKeeperPairFilter(dcClusterShard, backUpKeeper, target, keeperPairUsedInfoMap)) {
                updateSortedDescKeeperContainerUsedInfoModelQueue(anotherQueue, target.getKeeperIp(), target);
                target = null;
                if (keeperContainerMigrationDetail.getMigrateKeeperCount() != 0) result.add(keeperContainerMigrationDetail);
                continue;
            }
            keeperContainerMigrationDetail.addReadyToMigrateShard(dcClusterShard.getKey());
            target.setActiveInputFlow(target.getActiveInputFlow() + dcClusterShard.getValue().getInputFlow()).setTotalRedisUsedMemory(target.getTotalRedisUsedMemory() + dcClusterShard.getValue().getPeerData());
            updateIpPair(src, backUpKeeper, target, dcClusterShard);
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
        logger.info("[analyzeKeeperContainerUsedInfo] config keeperPairOverLoadFactor:{}", keeperPairOverLoadFactor);
        KeeperContainerOverloadStandardModel minStandardModel = new KeeperContainerOverloadStandardModel()
                .setFlowOverload((long) (Math.min(pairB.getInputFlowStandard(), pairA.getInputFlowStandard()) * keeperPairOverLoadFactor))
                .setPeerDataOverload((long) (Math.min(pairB.getRedisUsedMemoryStandard(), pairA.getRedisUsedMemoryStandard()) * keeperPairOverLoadFactor));

        IPPairData longLongPair = keeperPairUsedInfoMap.get(new IPPair(pairA.getKeeperIp(), pairB.getKeeperIp()));
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
        List<Map.Entry<DcClusterShardActive, KeeperUsedInfo>> allDcClusterShards = getDescDcClusterShardDetails(ipPairMap.get(pairA.getKeeperIp()).get(pairB.getKeeperIp()), isPeerDataOverload);
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
            if (!keeperContainerFilterChain.doKeeperPairFilter(dcClusterShard, srcKeeperContainer, target, keeperPairUsedInfoMap)) {
                usedTarget.add(target);
                target = null;
                generateResult(result, keeperContainerDetailModel);
                continue;
            }
            keeperContainerDetailModel.addReadyToMigrateShard(dcClusterShard.getKey());
            updateIpPair(srcKeeperContainer, backUpKeeperContainer, target, dcClusterShard);
            overloadData = updateOverLoadData(isPeerDataOverload, overloadData, dcClusterShard.getValue());
        }

        if (target != null) {
            usedTarget.add(target);
        }

        generateResult(result, keeperContainerDetailModel);
        availableKeeperContainers.addAll(usedTarget);

        return result;
    }

    private void updateIpPair(KeeperContainerUsedInfoModel srcKeeper, KeeperContainerUsedInfoModel backUpKeeper,
                              KeeperContainerUsedInfoModel target,
                              Map.Entry<DcClusterShardActive, KeeperUsedInfo> dcClusterShard) {
        IPPair ipPair = new IPPair(srcKeeper.getKeeperIp(), backUpKeeper.getKeeperIp());
        IPPairData ipPairData = keeperPairUsedInfoMap.get(ipPair);
        if (ipPairData.getNumber() == 1) {
            keeperPairUsedInfoMap.remove(ipPair);
            ipPairMap.get(srcKeeper.getKeeperIp()).remove(backUpKeeper.getKeeperIp());
            ipPairMap.get(backUpKeeper.getKeeperIp()).remove(srcKeeper.getKeeperIp());
        } else {
            keeperPairUsedInfoMap.put(ipPair, ipPairData.subData(dcClusterShard.getValue().getInputFlow(), dcClusterShard.getValue().getPeerData()));
        }
        IPPair newIPpair = new IPPair(srcKeeper.getKeeperIp(), target.getKeeperIp());
        if (keeperPairUsedInfoMap.containsKey(newIPpair)) {
            IPPairData newIpPairData = keeperPairUsedInfoMap.get(newIPpair);
            keeperPairUsedInfoMap.put(newIPpair, newIpPairData.addData(dcClusterShard.getValue().getInputFlow(), dcClusterShard.getValue().getPeerData()));
        } else {
            if (!ipPairMap.containsKey(target.getKeeperIp())) {
                ipPairMap.put(target.getKeeperIp(), new HashMap<>());
            }
            ipPairMap.get(srcKeeper.getKeeperIp()).put(target.getKeeperIp(), new HashMap<>());
            ipPairMap.get(target.getKeeperIp()).put(srcKeeper.getKeeperIp(), new HashMap<>());
            ipPairMap.get(srcKeeper.getKeeperIp()).get(target.getKeeperIp()).put(dcClusterShard.getKey(), dcClusterShard.getValue());
            ipPairMap.get(target.getKeeperIp()).get(srcKeeper.getKeeperIp()).put(dcClusterShard.getKey(), dcClusterShard.getValue());
            keeperPairUsedInfoMap.put(newIPpair, new IPPairData(dcClusterShard.getValue().getInputFlow(), dcClusterShard.getValue().getPeerData(), 1));
        }

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
        List<Map.Entry<DcClusterShardActive, KeeperUsedInfo>> allDescDcClusterShards;
        if (isPeerDataOverload) {
            allDescDcClusterShards = allDetailInfo.entrySet().stream()
                    .sorted((o1, o2) -> (int) (o2.getValue().getPeerData() - o1.getValue().getPeerData())).collect(Collectors.toList());
        } else {
            allDescDcClusterShards = allDetailInfo.entrySet().stream()
                    .sorted((o1, o2) -> (int) (o2.getValue().getInputFlow() - o1.getValue().getInputFlow())).collect(Collectors.toList());
        }
        return allDescDcClusterShards;
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

    private KeeperContainerUsedInfoModel getBackUpKeeper(DcClusterShard dcClusterShard) {
        DcClusterShardActive backUpDcClusterShardActive = new DcClusterShardActive(dcClusterShard, false);
        KeeperUsedInfo keeperUsedInfo = allDetailInfo.get(backUpDcClusterShardActive);
        String backUpKeeperContainerIp = keeperUsedInfo.getKeeperIP();
        return currentDcAllKeeperContainerUsedInfoModelMap.get(backUpKeeperContainerIp);
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
    Map<Integer, Date> getCheckerIndexes() {
        return checkerIndexes;
    }

    @VisibleForTesting
    void setExecutors(Executor executors){
        this.executors = executors;
    }

    @VisibleForTesting
    void setKeeperContainerService(KeeperContainerService service){
        this.keeperContainerService = service;
    }

    @VisibleForTesting
    void setKeeperContainerFilterChain(KeeperContainerFilterChain keeperContainerFilterChain){
        this.keeperContainerFilterChain = keeperContainerFilterChain;
    }

    @Override
    public Map<Integer, List<KeeperContainerUsedInfoModel>> getKeeperContainerUsedInfoModelIndexMap() {
        return keeperContainerUsedInfoModelIndexMap;
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

    class KeeperContainerInfoGetCommand extends AbstractCommand<List<KeeperContainerUsedInfoModel>> {

        private String domain;
        private RestOperations restTemplate;

        public KeeperContainerInfoGetCommand(String domain, RestOperations restTemplate) {
            this.domain = domain;
            this.restTemplate = restTemplate;
        }

        @Override
        public String getName() {
            return "getKeeperContainerInfo";
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                ResponseEntity<List<KeeperContainerUsedInfoModel>> result =
                        restTemplate.exchange(domain + "/api/keepercontainer/info/all", HttpMethod.GET, null,
                                new ParameterizedTypeReference<List<KeeperContainerUsedInfoModel>>() {});
                future().setSuccess(result.getBody());
            } catch (Throwable th) {
                getLogger().error("get keeper container info:{} fail", domain, th);
                future().setFailure(th);
            }
        }

        @Override
        protected void doReset() {

        }
    }

    class KeeperContainerFullSynchronizationTimeGetCommand extends AbstractCommand<Integer> {

        private String domain;
        private RestOperations restTemplate;

        public KeeperContainerFullSynchronizationTimeGetCommand(String domain, RestOperations restTemplate) {
            this.domain = domain;
            this.restTemplate = restTemplate;
        }

        @Override
        public String getName() {
            return "getKeeperContainerFullSynchronizationTime";
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                ResponseEntity<Integer> result =
                        restTemplate.exchange(domain + "/api/keepercontainer/full/synchronization/time", HttpMethod.GET, null,
                                new ParameterizedTypeReference<Integer>() {});
                future().setSuccess(result.getBody());
            } catch (Throwable th) {
                getLogger().error("get keeper container info:{} fail", domain, th);
                future().setFailure(th);
            }
        }

        @Override
        protected void doReset() {

        }
    }



    public static class IPPair {
        private String ip1;
        private String ip2;

        public IPPair(String ip1, String ip2) {
            this.ip1 = ip1;
            this.ip2 = ip2;
        }

        public String getIp1() {
            return ip1;
        }

        public String getIp2() {
            return ip2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IPPair ipPair = (IPPair) o;
            return Objects.equals(ip1, ipPair.ip1) &&
                    Objects.equals(ip2, ipPair.ip2) ||
                    Objects.equals(ip1, ipPair.ip2) &&
                            Objects.equals(ip2, ipPair.ip1);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip1, ip2) + Objects.hash(ip2, ip1);
        }
    }

    public static class IPPairData {
        private long inputFlow;
        private long peerData;
        private int number;

        public IPPairData() {
        }

        public IPPairData(long inputFlow, long peerData, int number) {
            this.inputFlow = inputFlow;
            this.peerData = peerData;
            this.number = number;
        }

        public IPPairData addData(long inputFlow, long peerData) {
            this.inputFlow += inputFlow;
            this.peerData += peerData;
            this.number++;
            return this;
        }

        public IPPairData subData(long inputFlow, long peerData) {
            this.inputFlow -= inputFlow;
            this.peerData -= peerData;
            this.number--;
            return this;
        }


        public long getInputFlow() {
            return inputFlow;
        }

        public void setInputFlow(long inputFlow) {
            this.inputFlow = inputFlow;
        }

        public long getPeerData() {
            return peerData;
        }

        public void setPeerData(long peerData) {
            this.peerData = peerData;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }
    }

}
