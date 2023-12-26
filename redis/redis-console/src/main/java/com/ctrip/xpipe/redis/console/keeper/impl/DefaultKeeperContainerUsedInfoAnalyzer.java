package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.monitor.CatEventMonitor;
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

    private Map<Integer, List<KeeperContainerUsedInfoModel>> allKeeperContainerUsedInfoModels = new HashMap<>();

    private List<KeeperContainerUsedInfoModel> allKeeperContainerUsedInfoModelsList = new ArrayList<>();

    private List<MigrationKeeperContainerDetailModel> allDcKeeperContainerDetailModel = new ArrayList<>();

    private long maxKeeperContainerActiveRedisUsedMemory;

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
    public List<KeeperContainerUsedInfoModel> getAllKeeperContainerUsedInfoModelsList() {
        List<KeeperContainerUsedInfoModel> result = Collections.synchronizedList(new ArrayList<>());
        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        config.getConsoleDomains().forEach((dc, domains) -> {
            if (currentDc.equalsIgnoreCase(dc)) {
                result.addAll(getAllDcKeeperContainerUsedInfoModelsList());
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
    public List<KeeperContainerUsedInfoModel> getAllDcKeeperContainerUsedInfoModelsList() {
        return allKeeperContainerUsedInfoModelsList;
    }

    @Override
    public int getMaxKeeperContainerFullSynchronizationTime() {
        AtomicInteger result = new AtomicInteger();
        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        config.getConsoleDomains().forEach((dc, domains) -> {
            if (currentDc.equalsIgnoreCase(dc)) {
                result.set(Math.max(result.get(), getAllDcMaxKeeperContainerFullSynchronizationTime()));
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
    public int getAllDcMaxKeeperContainerFullSynchronizationTime() {
        double keeperContainerIoRate = config.getKeeperContainerIoRate();
        return (int) (maxKeeperContainerActiveRedisUsedMemory/1024/1024/keeperContainerIoRate/60);
    }

    @Override
    public synchronized void updateKeeperContainerUsedInfo(int index, List<KeeperContainerUsedInfoModel> keeperContainerUsedInfoModels) {
        if (keeperContainerUsedInfoModels != null && !keeperContainerUsedInfoModels.isEmpty()){
            allKeeperContainerUsedInfoModels.put(index, keeperContainerUsedInfoModels);
        }

        Date currentTime = new Date();
        checkerIndexes.put(index, currentTime);
        removeExpireData(currentTime);
        if (!checkDataIntegrity()) return;

        allKeeperContainerUsedInfoModelsList.clear();
        allKeeperContainerUsedInfoModels.values().forEach(allKeeperContainerUsedInfoModelsList::addAll);

        allKeeperContainerUsedInfoModels.clear();
        checkerIndexes.clear();
        logger.info("[analyzeKeeperContainerUsedInfo] start analyze allKeeperContainerUsedInfoModelsList {}", allKeeperContainerUsedInfoModelsList);
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
            allKeeperContainerUsedInfoModels.remove(index);
            checkerIndexes.remove(index);
        }
    }

    private boolean checkDataIntegrity() {
        logger.info("[analyzeKeeperContainerUsedInfo] current index {}", checkerIndexes);
        return checkerIndexes.size() == config.getClusterDividedParts();
    }

    @VisibleForTesting
    void analyzeKeeperContainerUsedInfo() {
        Map<DcClusterShardActive, KeeperUsedInfo> allDetailInfo = analyzeKeeperPair();
        Map<String, KeeperContainerOverloadStandardModel> standardMap = analyzeKeeperContainerStandard();
        Map<String, KeeperContainerUsedInfoModel> keeperUsedInfoMap = getKeeperUsedInfoMap();
        logger.info("[analyzeKeeperContainerUsedInfo] standardMap {}", standardMap);
        logger.info("[analyzeKeeperContainerUsedInfo] keeperPairUsedInfoMap {}", keeperPairUsedInfoMap);
        logger.info("[analyzeKeeperContainerUsedInfo] ipPairMap {}", ipPairMap);
        PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers = new PriorityQueue<>(allKeeperContainerUsedInfoModelsList.size(),
                (keeper1, keeper2) -> (int)(keeper1.getActiveInputFlow() - keeper2.getActiveInputFlow()));
        PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers = new PriorityQueue<>(allKeeperContainerUsedInfoModelsList.size(),
                (keeper1, keeper2) -> (int)(keeper1.getTotalRedisUsedMemory() - keeper2.getTotalRedisUsedMemory()));
        allKeeperContainerUsedInfoModelsList.forEach(keeperContainerInfoModel -> {
            if (keeperContainerFilterChain.doKeeperContainerFilter(keeperContainerInfoModel, standardMap.get(keeperContainerInfoModel.getKeeperIp()))) {
                minPeerDataKeeperContainers.add(keeperContainerInfoModel);
                minInputFlowKeeperContainers.add(keeperContainerInfoModel);
            }
        });
        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        double keeperPairOverLoadFactor = config.getKeeperPairOverLoadFactor();
        logger.info("[analyzeKeeperContainerUsedInfo] config keeperPairOverLoadFactor:{}", keeperPairOverLoadFactor);
        for (KeeperContainerUsedInfoModel infoModel : allKeeperContainerUsedInfoModelsList) {
            KeeperContainerOverloadStandardModel standardModel = standardMap.get(infoModel.getKeeperIp());
            List<MigrationKeeperContainerDetailModel> migrationKeeperDetails = getOverloadKeeperMigrationDetails(infoModel,
                    standardModel, standardMap, keeperUsedInfoMap, allDetailInfo, minInputFlowKeeperContainers, minPeerDataKeeperContainers);
            if (migrationKeeperDetails != null){
                result.addAll(migrationKeeperDetails);
            }
            List<String> keeperPairIps = getIPPairsByIp(infoModel.getKeeperIp());
            for (String ip : keeperPairIps) {
                KeeperContainerOverloadStandardModel pairStandardModel = standardMap.get(ip);
                KeeperContainerOverloadStandardModel minStandardModel = new KeeperContainerOverloadStandardModel()
                        .setFlowOverload((long) (Math.min(pairStandardModel.getFlowOverload(), standardModel.getFlowOverload()) * keeperPairOverLoadFactor))
                        .setPeerDataOverload((long) (Math.min(pairStandardModel.getPeerDataOverload(), standardModel.getPeerDataOverload()) * keeperPairOverLoadFactor));
                List<MigrationKeeperContainerDetailModel> keeperPairMigrationKeeperDetails =
                        getKeeperPairMigrationKeeperDetails(infoModel, keeperUsedInfoMap.get(ip), minStandardModel, standardMap, minInputFlowKeeperContainers, minPeerDataKeeperContainers);
                if (keeperPairMigrationKeeperDetails != null){
                    result.addAll(keeperPairMigrationKeeperDetails);
                }
            }
        }
        allDcKeeperContainerDetailModel = result;
    }

    private Map<DcClusterShardActive, KeeperUsedInfo> analyzeKeeperPair(){
        keeperPairUsedInfoMap.clear();
        ipPairMap.clear();
        maxKeeperContainerActiveRedisUsedMemory = 0;
        Map<DcClusterShardActive, KeeperUsedInfo> allDetailInfo = new HashMap<>();
        for (KeeperContainerUsedInfoModel infoModel : allKeeperContainerUsedInfoModelsList) {
            if (infoModel.getActiveRedisUsedMemory() > maxKeeperContainerActiveRedisUsedMemory) {
                maxKeeperContainerActiveRedisUsedMemory = infoModel.getActiveRedisUsedMemory();
            }
            if (infoModel.getDetailInfo() != null) {
                allDetailInfo.putAll(infoModel.getDetailInfo());
            }
        }
        for (Map.Entry<DcClusterShardActive, KeeperUsedInfo> entry : allDetailInfo.entrySet()) {
            if (!entry.getKey().isActive()) continue;
            KeeperUsedInfo activeKeeperUsedInfo = entry.getValue();
            KeeperUsedInfo backUpKeeperUsedInfo = allDetailInfo.get(new DcClusterShardActive(entry.getKey().getDcId(), entry.getKey().getClusterId(), entry.getKey().getShardId(), false));
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
        return allDetailInfo;
    }

    private void generateIpPairMap(Map.Entry<DcClusterShardActive, KeeperUsedInfo> entry, KeeperUsedInfo activeKeeperUsedInfo, KeeperUsedInfo backUpKeeperUsedInfo) {
        if (!ipPairMap.containsKey(activeKeeperUsedInfo.getKeeperIP())) {
            ipPairMap.put(activeKeeperUsedInfo.getKeeperIP(), new HashMap<>());
        }
        if (!ipPairMap.get(activeKeeperUsedInfo.getKeeperIP()).containsKey(backUpKeeperUsedInfo.getKeeperIP())) {
            ipPairMap.get(activeKeeperUsedInfo.getKeeperIP()).put(backUpKeeperUsedInfo.getKeeperIP(), new HashMap<>());
        }
        ipPairMap.get(activeKeeperUsedInfo.getKeeperIP()).get(backUpKeeperUsedInfo.getKeeperIP()).put(entry.getKey(), entry.getValue());
    }

    private List<String> getIPPairsByIp(String ip) {
        List<String> ipPairs = new ArrayList<>();
        if (ipPairMap.containsKey(ip)) {
            ipPairs.addAll(ipPairMap.get(ip).keySet());
        }
        return ipPairs;
    }

    private Map<String, KeeperContainerUsedInfoModel> getKeeperUsedInfoMap() {
        Map<String, KeeperContainerUsedInfoModel> map = new HashMap<>();
        allKeeperContainerUsedInfoModelsList.forEach(value -> map.put(value.getKeeperIp(), value));
        return map;
    }

    @VisibleForTesting
    Map<String, KeeperContainerOverloadStandardModel> analyzeKeeperContainerStandard() {
        Map<String, KeeperContainerOverloadStandardModel> overloadStandardModelMap = new HashMap<>();
        KeeperContainerOverloadStandardModel keeperContainerOverloadStandard = config.getKeeperContainerOverloadStandards().get(currentDc);
        double loadFactor = config.getKeeperContainerOverloadFactor();
        KeeperContainerOverloadStandardModel defaultOverloadStandard = getDefaultStandard(keeperContainerOverloadStandard, loadFactor);
        logger.debug("[analyzeKeeperContainerUsedInfo] keeperContainerDefaultOverloadStandard: {}", defaultOverloadStandard);
        for (KeeperContainerUsedInfoModel keeperContainerUsedInfoModel : allKeeperContainerUsedInfoModelsList) {
            KeeperContainerOverloadStandardModel realKeeperContainerOverloadStandard = getRealStandard(keeperContainerOverloadStandard, defaultOverloadStandard, keeperContainerUsedInfoModel, loadFactor);
            keeperContainerUsedInfoModel.setInputFlowStandard(realKeeperContainerOverloadStandard.getFlowOverload());
            keeperContainerUsedInfoModel.setRedisUsedMemoryStandard(realKeeperContainerOverloadStandard.getPeerDataOverload());
            overloadStandardModelMap.put(keeperContainerUsedInfoModel.getKeeperIp(), realKeeperContainerOverloadStandard);
        }
        return overloadStandardModelMap;
    }

    private KeeperContainerOverloadStandardModel getRealStandard(KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                                                                 KeeperContainerOverloadStandardModel defaultOverloadStandard,
                                                                 KeeperContainerUsedInfoModel keeperContainerUsedInfoModel,
                                                                 double loadFactor){
        KeepercontainerTbl keepercontainerTbl = keeperContainerService.find(keeperContainerUsedInfoModel.getKeeperIp());
        keeperContainerUsedInfoModel.setDiskType(keepercontainerTbl.getKeepercontainerDiskType());
        keeperContainerUsedInfoModel.setKeeperContainerActive(keepercontainerTbl.isKeepercontainerActive());
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

    private KeeperContainerOverloadStandardModel getDefaultStandard(KeeperContainerOverloadStandardModel keeperContainerOverloadStandard, double loadFactor){
        if (keeperContainerOverloadStandard == null) {
            return new KeeperContainerOverloadStandardModel()
                    .setFlowOverload((long) (DEFAULT_KEEPER_FLOW_OVERLOAD * loadFactor)).setPeerDataOverload((long) (DEFAULT_PEER_DATA_OVERLOAD * loadFactor));
        } else {
            return new KeeperContainerOverloadStandardModel()
                    .setFlowOverload((long) (keeperContainerOverloadStandard.getFlowOverload() * loadFactor)).setPeerDataOverload((long) (keeperContainerOverloadStandard.getFlowOverload() * loadFactor));
        }
    }

    private List<MigrationKeeperContainerDetailModel> getOverloadKeeperMigrationDetails(KeeperContainerUsedInfoModel src,
                                                                                        KeeperContainerOverloadStandardModel srcStandard,
                                                                                        Map<String, KeeperContainerOverloadStandardModel> standardMap,
                                                                                        Map<String, KeeperContainerUsedInfoModel> keeperUsedInfoMap,
                                                                                        Map<DcClusterShardActive, KeeperUsedInfo> allDetailInfo,
                                                                                        PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers,
                                                                                        PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers) {

        long overloadInputFlow = src.getActiveInputFlow() - srcStandard.getFlowOverload();
        long overloadPeerData = src.getActiveRedisUsedMemory() - srcStandard.getPeerDataOverload();
        KeeperContainerOverloadCause overloadCause = getKeeperContainerOverloadCause(overloadInputFlow, overloadPeerData);
        if (overloadCause == null) return null;

        switch (overloadCause) {
            case PEER_DATA_OVERLOAD:
                return getOverloadKeeperMigrationDetails(src, true, overloadPeerData, srcStandard, standardMap, keeperUsedInfoMap, allDetailInfo, minPeerDataKeeperContainers, overloadCause);
            case INPUT_FLOW_OVERLOAD:
            case BOTH:
                return getOverloadKeeperMigrationDetails(src, false, overloadInputFlow, srcStandard, standardMap, keeperUsedInfoMap, allDetailInfo, minInputFlowKeeperContainers, overloadCause);
            default:
                logger.warn("invalid keeper container overload cause {}", overloadCause);
                return null;
        }
    }

    private List<MigrationKeeperContainerDetailModel> getOverloadKeeperMigrationDetails(KeeperContainerUsedInfoModel src,
                                                                                        boolean isPeerDataOverload,
                                                                                        long overloadData, KeeperContainerOverloadStandardModel srcStandard,
                                                                                        Map<String, KeeperContainerOverloadStandardModel> standardMap,
                                                                                        Map<String, KeeperContainerUsedInfoModel> keeperUsedInfoMap,
                                                                                        Map<DcClusterShardActive, KeeperUsedInfo> allDetailInfo,
                                                                                        PriorityQueue<KeeperContainerUsedInfoModel> availableKeeperContainers,
                                                                                        KeeperContainerOverloadCause overloadCause) {
        List<Map.Entry<DcClusterShardActive, KeeperUsedInfo>> allDcClusterShards;
        if (isPeerDataOverload) {
            allDcClusterShards = src.getDetailInfo().entrySet().stream()
                    .sorted((o1, o2) -> (int) (o2.getValue().getPeerData() - o1.getValue().getPeerData())).collect(Collectors.toList());
        } else {
            allDcClusterShards = src.getDetailInfo().entrySet().stream()
                    .sorted((o1, o2) -> (int) (o2.getValue().getInputFlow() - o1.getValue().getInputFlow())).collect(Collectors.toList());
        }

        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        KeeperContainerUsedInfoModel target = null;
        MigrationKeeperContainerDetailModel keeperContainerDetailModel = null;
        MigrationKeeperContainerDetailModel switchActiveDetail = null;

        logger.debug("[analyzeKeeperContainerUsedInfo] src: {}, overlaodCause:{}, overloadData:{}, availableKeeperContainers:{} ",
                src, isPeerDataOverload, overloadData, availableKeeperContainers);

        for (Map.Entry<DcClusterShardActive, KeeperUsedInfo> dcClusterShard : allDcClusterShards) {
            if (!dcClusterShard.getKey().isActive()) continue;
            String backUpKeeperContainerIp = allDetailInfo.get(new DcClusterShardActive(dcClusterShard.getKey().getDcId(), dcClusterShard.getKey().getClusterId(), dcClusterShard.getKey().getShardId(), false)).getKeeperIP();
            KeeperContainerUsedInfoModel backUpKeeperUsedInfoModel = keeperUsedInfoMap.get(backUpKeeperContainerIp);
            KeeperContainerOverloadStandardModel backUpKeeperStandard = standardMap.get(backUpKeeperContainerIp);
            if (keeperContainerFilterChain.doKeeperContainerFilter(backUpKeeperUsedInfoModel, backUpKeeperStandard) &&
                    keeperContainerFilterChain.doKeeperFilter(dcClusterShard, backUpKeeperUsedInfoModel, srcStandard, backUpKeeperStandard, keeperPairUsedInfoMap)) {
                if (switchActiveDetail == null) {
                    switchActiveDetail = new MigrationKeeperContainerDetailModel(src, null, 0, true, false, overloadCause.name(), new ArrayList<>());
                }
                switchActiveDetail.addReadyToMigrateShard(dcClusterShard.getKey());
                if (target == null || !backUpKeeperUsedInfoModel.getKeeperIp().equals(target.getKeeperIp())) {
                    Queue<KeeperContainerUsedInfoModel> temp = new LinkedList<>();
                    while (!availableKeeperContainers.isEmpty()) {
                        KeeperContainerUsedInfoModel infoModel = availableKeeperContainers.poll();
                        if (infoModel.getKeeperIp().equals(backUpKeeperContainerIp)) {
                            temp.add(new KeeperContainerUsedInfoModel(backUpKeeperUsedInfoModel, dcClusterShard));
                            break;
                        }
                        temp.add(infoModel);
                    }
                    while(!temp.isEmpty()) {
                        availableKeeperContainers.add(temp.poll());
                    }
                } else {
                    target = new KeeperContainerUsedInfoModel(backUpKeeperUsedInfoModel, dcClusterShard);
                }
                long currentOverLoadData = isPeerDataOverload ? dcClusterShard.getValue().getPeerData() : dcClusterShard.getValue().getInputFlow();
                if ((overloadData -= currentOverLoadData) <= 0) break;
                continue;
            }
            if (target == null ) {
                target = availableKeeperContainers.poll();
                if (target == null) {
                    logger.warn("[analyzeKeeperContainerUsedInfo] no available keeper containers {} for overload keeper container {}",
                            availableKeeperContainers, src);
                    CatEventMonitor.DEFAULT.logEvent(KEEPER_RESOURCE_LACK, currentDc);
                    if (switchActiveDetail != null) result.add(switchActiveDetail);
                    return result;
                }
                keeperContainerDetailModel = new MigrationKeeperContainerDetailModel(src, target, 0, false, false, overloadCause.name(), new ArrayList<>());
            }
            if (!keeperContainerFilterChain.doKeeperFilter(dcClusterShard, target, srcStandard, standardMap.get(target.getKeeperIp()), keeperPairUsedInfoMap)) {
                target = null;
                if (keeperContainerDetailModel.getMigrateKeeperCount() != 0) result.add(keeperContainerDetailModel);
                continue;
            }
            keeperContainerDetailModel.addReadyToMigrateShard(dcClusterShard.getKey());
            target.setActiveInputFlow(target.getActiveInputFlow() + dcClusterShard.getValue().getInputFlow()).setTotalRedisUsedMemory(target.getTotalRedisUsedMemory() + dcClusterShard.getValue().getPeerData());
            IPPair ipPair = new IPPair(src.getKeeperIp(), backUpKeeperContainerIp);
            IPPairData ipPairData = keeperPairUsedInfoMap.get(ipPair);
            if (ipPairData.getNumber() == 1) {
                keeperPairUsedInfoMap.remove(ipPair);
                ipPairMap.get(src.getKeeperIp()).remove(backUpKeeperContainerIp);
                ipPairMap.get(backUpKeeperContainerIp).remove(src.getKeeperIp());
            } else {
                keeperPairUsedInfoMap.put(ipPair, ipPairData.subData(dcClusterShard.getValue().getInputFlow(), dcClusterShard.getValue().getPeerData()));
            }
            IPPair newIPpair = new IPPair(src.getKeeperIp(), target.getKeeperIp());
            if (keeperPairUsedInfoMap.containsKey(newIPpair)) {
                IPPairData newIpPairData = keeperPairUsedInfoMap.get(newIPpair);
                keeperPairUsedInfoMap.put(newIPpair, newIpPairData.addData(dcClusterShard.getValue().getInputFlow(), dcClusterShard.getValue().getPeerData()));
            } else {
                if (!ipPairMap.containsKey(target.getKeeperIp())) {
                    ipPairMap.put(target.getKeeperIp(), new HashMap<>());
                }
                ipPairMap.get(src.getKeeperIp()).put(target.getKeeperIp(), new HashMap<>());
                ipPairMap.get(target.getKeeperIp()).put(src.getKeeperIp(), new HashMap<>());
                ipPairMap.get(src.getKeeperIp()).get(target.getKeeperIp()).put(dcClusterShard.getKey(), dcClusterShard.getValue());
                ipPairMap.get(target.getKeeperIp()).get(src.getKeeperIp()).put(dcClusterShard.getKey(), dcClusterShard.getValue());
                keeperPairUsedInfoMap.put(newIPpair, new IPPairData(dcClusterShard.getValue().getInputFlow(), dcClusterShard.getValue().getPeerData(), 1));
            }
            long currentOverLoadData = isPeerDataOverload ? dcClusterShard.getValue().getPeerData() : dcClusterShard.getValue().getInputFlow();
            if ((overloadData -= currentOverLoadData) <= 0) break;
        }

        if (keeperContainerDetailModel != null && keeperContainerDetailModel.getMigrateKeeperCount() != 0) {
            result.add(keeperContainerDetailModel);
        }

        if (switchActiveDetail != null && switchActiveDetail.getMigrateKeeperCount() != 0) {
            result.add(switchActiveDetail);
        }


        if (target != null && target.getActiveInputFlow() < srcStandard.getPeerDataOverload()
                && target.getTotalRedisUsedMemory() < srcStandard.getPeerDataOverload()) {
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

    private List<MigrationKeeperContainerDetailModel> getKeeperPairMigrationKeeperDetails(KeeperContainerUsedInfoModel pairA,
                                                                                          KeeperContainerUsedInfoModel pairB,
                                                                                          KeeperContainerOverloadStandardModel minStandardModel,
                                                                                          Map<String, KeeperContainerOverloadStandardModel> standardMap,
                                                                                          PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers,
                                                                                          PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers){

        IPPairData longLongPair = keeperPairUsedInfoMap.get(new IPPair(pairA.getKeeperIp(), pairB.getKeeperIp()));
        long overloadInputFlow = longLongPair.getInputFlow() - minStandardModel.getFlowOverload();
        long overloadPeerData = longLongPair.getPeerData() - minStandardModel.getPeerDataOverload();
        KeeperContainerOverloadCause overloadCause = getKeeperPairOverloadCause(overloadInputFlow, overloadPeerData);
        if (overloadCause == null) return null;

        switch (overloadCause) {
            case KEEPER_PAIR_PEER_DATA_OVERLOAD:
                return getKeeperPairMigrationDetails(pairA, pairB, true, overloadPeerData, standardMap, minPeerDataKeeperContainers, overloadCause);
            case KEEPER_PAIR_INPUT_FLOW_OVERLOAD:
            case KEEPER_PAIR_BOTH:
                return getKeeperPairMigrationDetails(pairA, pairB, false, overloadInputFlow, standardMap, minInputFlowKeeperContainers, overloadCause);
            default:
                logger.warn("invalid keeper container overload cause {}", overloadCause);
                return null;
        }

    }

    private List<MigrationKeeperContainerDetailModel> getKeeperPairMigrationDetails(KeeperContainerUsedInfoModel pairA,
                                                                                    KeeperContainerUsedInfoModel pairB,
                                                                                    boolean isPeerDataOverload,
                                                                                    long overloadData,
                                                                                    Map<String, KeeperContainerOverloadStandardModel> standardMap,
                                                                                    PriorityQueue<KeeperContainerUsedInfoModel> availableKeeperContainers,
                                                                                    KeeperContainerOverloadCause overloadCause){
        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        if (keeperPairUsedInfoMap.get(new IPPair(pairA.getKeeperIp(), pairB.getKeeperIp())).getNumber() == 1) return result;
        List<Map.Entry<DcClusterShardActive, KeeperUsedInfo>> allDcClusterShards;
        Map<DcClusterShardActive, KeeperUsedInfo> dcClusterShardActiveKeeperUsedInfoMap = ipPairMap.get(pairA.getKeeperIp()).get(pairB.getKeeperIp());
        if (isPeerDataOverload) {
            allDcClusterShards = dcClusterShardActiveKeeperUsedInfoMap.entrySet().stream()
                    .sorted((o1, o2) -> (int) (o2.getValue().getPeerData() - o1.getValue().getPeerData())).collect(Collectors.toList());
        } else {
            allDcClusterShards = dcClusterShardActiveKeeperUsedInfoMap.entrySet().stream()
                    .sorted((o1, o2) -> (int) (o2.getValue().getInputFlow() - o1.getValue().getInputFlow())).collect(Collectors.toList());
        }
        KeeperContainerUsedInfoModel target = null;
        List<KeeperContainerUsedInfoModel> usedTarget = new ArrayList<>();
        MigrationKeeperContainerDetailModel keeperContainerDetailModel = null;

        logger.debug("[analyzeKeeperPairOverLoad] pairA: {}, pairB: {}, overloadCause:{}, overloadData:{}, availableKeeperContainers:{} ",
                pairA, pairB, isPeerDataOverload, overloadData, availableKeeperContainers);
        IPPair ipPair = new IPPair(pairA.getKeeperIp(), pairB.getKeeperIp());
        IPPairData ipPairData = keeperPairUsedInfoMap.get(ipPair);
        for (Map.Entry<DcClusterShardActive, KeeperUsedInfo> dcClusterShard : allDcClusterShards) {
            if (!dcClusterShard.getKey().isActive()) continue;
            String srcKeeperIp = dcClusterShard.getValue().getKeeperIP();
            KeeperContainerUsedInfoModel srcKeeperContainer = srcKeeperIp.equals(pairA.getKeeperIp()) ? pairA : pairB;
            if (target == null ) {
                target = availableKeeperContainers.poll();
                if (target == null) {
                    logger.warn("[analyzeKeeperContainerUsedInfo] no available keeper containers {} for overload keeper pair {},{}",
                            availableKeeperContainers, pairA, pairB);
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
            if (!keeperContainerFilterChain.doKeeperPairFilter(dcClusterShard, target, standardMap.get(srcKeeperIp), standardMap.get(target.getKeeperIp()), keeperPairUsedInfoMap)) {
                usedTarget.add(target);
                target = null;
                if (keeperContainerDetailModel.getMigrateKeeperCount() != 0) result.add(keeperContainerDetailModel);
                continue;
            }
            keeperContainerDetailModel.addReadyToMigrateShard(dcClusterShard.getKey());
            keeperPairUsedInfoMap.put(ipPair, ipPairData.subData(dcClusterShard.getValue().getInputFlow(), dcClusterShard.getValue().getPeerData()));

            if (keeperPairUsedInfoMap.containsKey(new IPPair(srcKeeperIp, target.getKeeperIp()))) {
                IPPairData newIpPairData = keeperPairUsedInfoMap.get(new IPPair(srcKeeperIp, target.getKeeperIp()));
                keeperPairUsedInfoMap.put(new IPPair(srcKeeperIp, target.getKeeperIp()), newIpPairData.addData(dcClusterShard.getValue().getInputFlow(), dcClusterShard.getValue().getPeerData()));
            } else {
                ipPairMap.get(srcKeeperIp).put(target.getKeeperIp(), new HashMap<>());
                ipPairMap.get(target.getKeeperIp()).put(srcKeeperIp, new HashMap<>());
                ipPairMap.get(srcKeeperIp).get(target.getKeeperIp()).put(dcClusterShard.getKey(), dcClusterShard.getValue());
                ipPairMap.get(target.getKeeperIp()).get(srcKeeperIp).put(dcClusterShard.getKey(), dcClusterShard.getValue());
                keeperPairUsedInfoMap.put(new IPPair(srcKeeperIp, target.getKeeperIp()), new IPPairData(dcClusterShard.getValue().getInputFlow(), dcClusterShard.getValue().getPeerData(), 1));
            }

            long currentOverLoadData = isPeerDataOverload ? dcClusterShard.getValue().getPeerData() : dcClusterShard.getValue().getInputFlow();
            if ((overloadData -= currentOverLoadData) <= 0) {
                usedTarget.add(target);
                break;
            }
        }

        if (keeperContainerDetailModel != null && keeperContainerDetailModel.getMigrateKeeperCount() != 0) {
            result.add(keeperContainerDetailModel);
        }
        availableKeeperContainers.addAll(usedTarget);

        return result;
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
