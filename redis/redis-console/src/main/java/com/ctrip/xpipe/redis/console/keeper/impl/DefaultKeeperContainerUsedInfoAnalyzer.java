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
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
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
import java.util.stream.Collectors;


public class DefaultKeeperContainerUsedInfoAnalyzer extends AbstractService implements KeeperContainerUsedInfoAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerUsedInfoAnalyzer.class);

    private static final long DEFAULT_PEER_DATA_OVERLOAD = 474 * 1024 * 1024 * 1024;

    private static final long DEFAULT_KEEPER_FLOW_OVERLOAD = 270 * 1024;

    private ConsoleConfig config;

    private KeeperContainerService keeperContainerService;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    private Map<Date, Integer> checkerIndexes = new TreeMap<>();

    private Map<Integer, List<KeeperContainerUsedInfoModel>> allKeeperContainerUsedInfoModels = new HashMap<>();

    private List<KeeperContainerUsedInfoModel> allKeeperContainerUsedInfoModelsList = new ArrayList<>();

    private List<MigrationKeeperContainerDetailModel> allDcKeeperContainerDetailModel = new ArrayList<>();

    Map<IPPair, Pair<Long, Long>> keeperPairUsedInfoMap = new HashMap<>();

    Map<String, Set<String>> ipMapSet = new HashMap<>();

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    private static final String KEEPER_RESOURCE_LACK = "keeper_resource_lack";

    public DefaultKeeperContainerUsedInfoAnalyzer() {}

    public DefaultKeeperContainerUsedInfoAnalyzer(ConsoleConfig config, KeeperContainerService keeperContainerService) {
        this.config = config;
        this.keeperContainerService = keeperContainerService;
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
        return allKeeperContainerUsedInfoModelsList;
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

        allKeeperContainerUsedInfoModelsList.clear();
        allKeeperContainerUsedInfoModels.values().forEach(allKeeperContainerUsedInfoModelsList::addAll);

        allKeeperContainerUsedInfoModels.clear();
        checkerIndexes.clear();

        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                analyzeKeeperContainerUsedInfo();
            }
        });
    }

    private void removeExpireData(Date currentTime) {
        Iterator<Map.Entry<Date, Integer>> iterator = checkerIndexes.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Date, Integer> entry = iterator.next();
            if (currentTime.getTime() - entry.getKey().getTime() > config.getKeeperCheckerIntervalMilli()) {
                logger.info("[removeExpireData] remove expire index:{} time:{}, expire time:{}", entry.getValue(), entry.getKey(), config.getKeeperCheckerIntervalMilli());
                allKeeperContainerUsedInfoModels.remove(entry.getValue());
                iterator.remove();
            } else break;
        }
    }

    private boolean checkDataIntegrity() {
        return checkerIndexes.size() == config.getClusterDividedParts();
    }

    @VisibleForTesting
    void analyzeKeeperContainerUsedInfo() {
        analyzeKeeperPair();
        Map<String, KeeperContainerOverloadStandardModel> standardMap = analyzeOverloadStandardModel();
        Map<String, KeeperContainerUsedInfoModel> keeperUsedInfoMap = getKeeperUsedInfoMap();
        logger.debug("[analyzeKeeperContainerUsedInfo] newKeeperContainerUsedInfoModels: {}", allKeeperContainerUsedInfoModelsList);
        PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers = new PriorityQueue<>(allKeeperContainerUsedInfoModelsList.size(),
                (keeper1, keeper2) -> (int)(keeper1.getActiveInputFlow() - keeper2.getActiveInputFlow()));
        PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers = new PriorityQueue<>(allKeeperContainerUsedInfoModelsList.size(),
                (keeper1, keeper2) -> (int)(keeper1.getRedisUsedMemory() - keeper2.getRedisUsedMemory()));
        allKeeperContainerUsedInfoModelsList.forEach(keeperContainerInfoModel -> {
            minPeerDataKeeperContainers.add(keeperContainerInfoModel);
            minInputFlowKeeperContainers.add(keeperContainerInfoModel);
        });
        List<MigrationKeeperContainerDetailModel> result = new ArrayList<>();
        double keeperPairOverLoadFactor = config.getKeeperPairOverLoadFactor();
        for (KeeperContainerUsedInfoModel infoModel : allKeeperContainerUsedInfoModelsList) {
            KeeperContainerOverloadStandardModel standardModel = standardMap.get(infoModel.getKeeperIp());
            List<MigrationKeeperContainerDetailModel> migrationKeeperDetails = getMigrationKeeperDetails(infoModel,
                    standardModel, minInputFlowKeeperContainers, minPeerDataKeeperContainers);
            if (migrationKeeperDetails != null){
                result.addAll(migrationKeeperDetails);
            }
            List<String> keeperPairIps = getIPPairsByIp(infoModel.getKeeperIp());
            for (String ip : keeperPairIps) {
                KeeperContainerOverloadStandardModel pairStandardModel = standardMap.get(ip);
                KeeperContainerOverloadStandardModel minStandardModel = new KeeperContainerOverloadStandardModel()
                        .setFlowOverload((long) (Math.min(pairStandardModel.getFlowOverload(), standardModel.getFlowOverload()) * keeperPairOverLoadFactor))
                        .setPeerDataOverload((long) (Math.min(pairStandardModel.getPeerDataOverload(), standardModel.getPeerDataOverload()) * keeperPairOverLoadFactor));
                List<MigrationKeeperContainerDetailModel> keeperPairMigrationKeeperDetails = getKeeperPairMigrationKeeperDetails(infoModel, keeperUsedInfoMap.get(ip), minStandardModel, minInputFlowKeeperContainers, minPeerDataKeeperContainers);
                if (keeperPairMigrationKeeperDetails != null){
                    result.addAll(keeperPairMigrationKeeperDetails);
                }
            }
        }
        allDcKeeperContainerDetailModel = result;
    }

    private void analyzeKeeperPair(){
        keeperPairUsedInfoMap.clear();
        ipMapSet.clear();
        Map<DcClusterShardActive, KeeperUsedInfo> allDetailInfo = new HashMap<>();
        for (KeeperContainerUsedInfoModel infoModel : allKeeperContainerUsedInfoModelsList) {
            allDetailInfo.putAll(infoModel.getDetailInfo());
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
            Pair<Long, Long> keeperPairUsedInfoMapValue = new Pair<>(activeKeeperUsedInfo.getInputFlow(), activeKeeperUsedInfo.getPeerData());
            if (keeperPairUsedInfoMap.containsKey(ipPair)) {
                Pair<Long, Long> value = keeperPairUsedInfoMap.get(ipPair);
                keeperPairUsedInfoMapValue = new Pair<>(activeKeeperUsedInfo.getInputFlow() + value.getKey(), activeKeeperUsedInfo.getPeerData() + value.getValue());
            }
            keeperPairUsedInfoMap.put(ipPair, keeperPairUsedInfoMapValue);

            if (!ipMapSet.containsKey(activeKeeperUsedInfo.getKeeperIP())) {
                ipMapSet.put(activeKeeperUsedInfo.getKeeperIP(), new HashSet<>());
            }
            ipMapSet.get(activeKeeperUsedInfo.getKeeperIP()).add(backUpKeeperUsedInfo.getKeeperIP());
            if (!ipMapSet.containsKey(backUpKeeperUsedInfo.getKeeperIP())) {
                ipMapSet.put(backUpKeeperUsedInfo.getKeeperIP(), new HashSet<>());
            }
            ipMapSet.get(backUpKeeperUsedInfo.getKeeperIP()).add(activeKeeperUsedInfo.getKeeperIP());
        }
    }

    private List<String> getIPPairsByIp(String ip) {
        List<String> ipPairs = new ArrayList<>();
        if (ipMapSet.containsKey(ip)) {
            ipPairs.addAll(ipMapSet.get(ip));
        }
        return ipPairs;
    }

    private Map<String, KeeperContainerUsedInfoModel> getKeeperUsedInfoMap() {
        Map<String, KeeperContainerUsedInfoModel> map = new HashMap<>();
        allKeeperContainerUsedInfoModelsList.forEach(value -> map.put(value.getKeeperIp(), value));
        return map;
    }

    private Map<String, KeeperContainerOverloadStandardModel> analyzeOverloadStandardModel() {
        Map<String, KeeperContainerOverloadStandardModel> overloadStandardModelMap = new HashMap<>();
        KeeperContainerOverloadStandardModel keeperContainerOverloadStandard = config.getKeeperContainerOverloadStandards().get(currentDc);
        double loadFactor = config.getKeeperContainerOverloadFactor();
        KeeperContainerOverloadStandardModel defaultOverloadStandard = getDefaultStandard(keeperContainerOverloadStandard, loadFactor);
        logger.debug("[analyzeKeeperContainerUsedInfo] keeperContainerDefaultOverloadStandard: {}", defaultOverloadStandard);
        for (KeeperContainerUsedInfoModel keeperContainerUsedInfoModel : allKeeperContainerUsedInfoModelsList) {
            KeeperContainerOverloadStandardModel realKeeperContainerOverloadStandard = getRealStandard(keeperContainerOverloadStandard, defaultOverloadStandard, keeperContainerUsedInfoModel, loadFactor);
            overloadStandardModelMap.put(keeperContainerUsedInfoModel.getKeeperIp(), realKeeperContainerOverloadStandard);
        }
        return overloadStandardModelMap;
    }

    private KeeperContainerOverloadStandardModel getRealStandard(KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                                                                 KeeperContainerOverloadStandardModel defaultOverloadStandard,
                                                                 KeeperContainerUsedInfoModel keeperContainerUsedInfoModel,
                                                                 double loadFactor){
        KeepercontainerTbl keepercontainerTbl = keeperContainerService.find(keeperContainerUsedInfoModel.getKeeperIp());
        KeeperContainerOverloadStandardModel realKeeperContainerOverloadStandard = defaultOverloadStandard;
        if (keeperContainerOverloadStandard != null && keeperContainerOverloadStandard.getDiskTypes() != null && !keeperContainerOverloadStandard.getDiskTypes().isEmpty()) {
            for (KeeperContainerOverloadStandardModel.DiskTypesEnum diskType : keeperContainerOverloadStandard.getDiskTypes()) {
                keeperContainerUsedInfoModel.setDiskType(keepercontainerTbl.getKeepercontainerDiskType());
                if (diskType.getDiskType().getDesc().equals(keepercontainerTbl.getKeepercontainerDiskType())) {
                    realKeeperContainerOverloadStandard =
                            new KeeperContainerOverloadStandardModel()
                                    .setFlowOverload((long) (diskType.getFlowOverload() * loadFactor))
                                    .setPeerDataOverload((long) (diskType.getPeerDataOverload() * loadFactor));
                }
            }
        } else {
            logger.warn("[analyzeKeeperContainerUsedInfo] keeperContainerOverloadStandard diskType {} from dc {} is null," +
                    " use default config", keepercontainerTbl.getKeepercontainerDiskType() ,currentDc);
        }
        return realKeeperContainerOverloadStandard;
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

    private List<MigrationKeeperContainerDetailModel> getMigrationKeeperDetails(KeeperContainerUsedInfoModel src,
                                                                                KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                                                                                PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers,
                                                                                PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers) {

        long overloadInputFlow = src.getActiveInputFlow() - keeperContainerOverloadStandard.getFlowOverload();
        long overloadPeerData = src.getRedisUsedMemory() - keeperContainerOverloadStandard.getPeerDataOverload();
        KeeperContainerOverloadCause overloadCause = getKeeperContainerOverloadCause(overloadInputFlow, overloadPeerData);
        if (overloadCause == null) return null;
        src.getOverLoadCause().add(overloadCause.name());

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
        List<Map.Entry<DcClusterShardActive, KeeperUsedInfo>> allDcClusterShards = null;
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

        logger.debug("[analyzeKeeperContainerUsedInfo] src: {}, overlaodCause:{}, overloadData:{}, availableKeeperContainers:{} ",
                src, isPeerDataOverload, overloadData, availableKeeperContainers);

        for (Map.Entry<DcClusterShardActive, KeeperUsedInfo> dcClusterShard : allDcClusterShards) {
            if (target == null ) {
                target = availableKeeperContainers.poll();
                if (target == null) {
                    logger.warn("[analyzeKeeperContainerUsedInfo] no available keeper containers {} for overload keeper container {}",
                            availableKeeperContainers, src);
                    CatEventMonitor.DEFAULT.logEvent(KEEPER_RESOURCE_LACK, currentDc);
                    return result;
                }

                keeperContainerDetailModel = new MigrationKeeperContainerDetailModel(
                        new KeeperContainerUsedInfoModel(src.getKeeperIp(),src.getDcName(), src.getActiveInputFlow(), src.getRedisUsedMemory()),
                        new KeeperContainerUsedInfoModel(target.getKeeperIp(),src.getDcName(), target.getActiveInputFlow(), target.getRedisUsedMemory()),
                        0, new ArrayList<>());
            }

            long currentOverLoadData = isPeerDataOverload ? dcClusterShard.getValue().getPeerData() : dcClusterShard.getValue().getInputFlow();

            long targetInputFlow = target.getActiveInputFlow() + dcClusterShard.getValue().getInputFlow();
            long targetPeerData = target.getRedisUsedMemory() + dcClusterShard.getValue().getPeerData();
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
            target.setActiveInputFlow(targetInputFlow).setRedisUsedMemory(targetPeerData);

            if ((overloadData -= currentOverLoadData) <= 0) break;
        }

        if (keeperContainerDetailModel != null && keeperContainerDetailModel.getMigrateKeeperCount() != 0) {
            result.add(keeperContainerDetailModel);
        }

        if (target != null && target.getActiveInputFlow() < keeperContainerOverloadStandard.getPeerDataOverload()
                && target.getRedisUsedMemory() < keeperContainerOverloadStandard.getPeerDataOverload()) {
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
                                                                                PriorityQueue<KeeperContainerUsedInfoModel> minInputFlowKeeperContainers,
                                                                                PriorityQueue<KeeperContainerUsedInfoModel> minPeerDataKeeperContainers){

        Pair<Long, Long> longLongPair = keeperPairUsedInfoMap.get(new IPPair(pairA.getKeeperIp(), pairB.getKeeperIp()));
        long overloadInputFlow = longLongPair.getKey() - minStandardModel.getFlowOverload();
        long overloadPeerData = longLongPair.getValue() - minStandardModel.getPeerDataOverload();
        KeeperContainerOverloadCause overloadCause = getKeeperPairOverloadCause(overloadInputFlow, overloadPeerData);
        if (overloadCause == null) return null;
        return null;
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

    class IPPair {
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
}
