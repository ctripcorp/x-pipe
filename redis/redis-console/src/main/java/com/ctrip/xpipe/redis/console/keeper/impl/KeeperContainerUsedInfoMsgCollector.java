package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.impl.KeeperContainerServiceImpl;
import com.ctrip.xpipe.redis.console.service.impl.OrganizationServiceImpl;
import com.ctrip.xpipe.redis.console.service.impl.RedisServiceImpl;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Component
public class KeeperContainerUsedInfoMsgCollector extends AbstractService implements ConsoleLeaderAware {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private KeeperContainerCheckerService keeperContainerService;

    @Autowired
    private KeeperContainerServiceImpl keeperContainerServiceImpl;

    @Autowired
    private OrganizationServiceImpl organizationService;

    @Autowired
    private RedisServiceImpl redisService;

    @Autowired
    private CheckerConfig config;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private KeeperContainerUsedInfoAnalyzer keeperContainerUsedInfoAnalyzer;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask keeperContainerUsedInfoMsgCollectorTask;

    private Map<String, Map<Integer, Pair<Map<HostPort, RedisMsg>, Date>>> redisMsgCache = new HashMap<>();

    protected MetricProxy metricProxy = MetricProxy.DEFAULT;

    private static final String TRAFFIC_TYPE = "keepercontainer.traffic";

    private static final String DATA_TYPE = "keepercontainer.data";

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();
    
    private static final Logger logger = LoggerFactory.getLogger(KeeperContainerUsedInfoMsgCollector.class);

    @PostConstruct
    public void init() {
        logger.debug("[postConstruct] start");
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("KeeperContainerUsedInfoMsgCollector"));
        this.keeperContainerUsedInfoMsgCollectorTask = new DynamicDelayPeriodTask("getAllCurrentDcRedisMsg",
                this::getAllCurrentDcRedisMsgAndCalculate, () -> config.getKeeperCheckerIntervalMilli(), scheduled);
    }

    public void saveMsg(int index, Map<String, Map<HostPort, RedisMsg>> redisMsgMap) {
        for (String dc : consoleConfig.getConsoleDomains().keySet()) {
            if (!redisMsgCache.containsKey(dc)) {
                redisMsgCache.put(dc, new HashMap<>());
            }
            Map<HostPort, RedisMsg> redisMsgDcMap = new HashMap<>();
            if (redisMsgMap != null && redisMsgMap.containsKey(dc)) {
                redisMsgDcMap = redisMsgMap.get(dc);
            }
            redisMsgCache.get(dc).put(index, new Pair<>(redisMsgDcMap, new Date()));
        }
    }

    private void getAllCurrentDcRedisMsgAndCalculate() {
        Map<HostPort, RedisMsg> allRedisMsg = getDcRedisMsg(currentDc);
        for (Map.Entry<String, String> entry : consoleConfig.getConsoleDomains().entrySet()) {
            ResponseEntity<Map<HostPort, RedisMsg>> result =
                    restTemplate.exchange(entry.getValue() + "/api/keepercontainer/redis/msg/" + currentDc, HttpMethod.GET, null,
                            new ParameterizedTypeReference<Map<HostPort, RedisMsg>>() {});
            if (result.getBody() != null) {
                allRedisMsg.putAll(result.getBody());
            }
        }
        if (allRedisMsg.isEmpty()) return;
        Map<String, KeeperContainerUsedInfoModel> modelMap = redisMsgMap2KeeperContainerUsedInfoModelsUtil(allRedisMsg);
        reportKeeperData(modelMap);
        keeperContainerUsedInfoAnalyzer.updateKeeperContainerUsedInfo(modelMap);
    }

    public Map<HostPort, RedisMsg> getDcRedisMsg(String dcName) {
        removeExpireData();
        Map<Integer, Pair<Map<HostPort, RedisMsg>, Date>> integerPairMap = redisMsgCache.get(dcName);
        if (integerPairMap.size() != consoleConfig.getClusterDividedParts()) {
            //数据不全告警
        }
        Map<HostPort, RedisMsg> result = new HashMap<>();
        integerPairMap.values().forEach(pair -> result.putAll(pair.getKey()));
        return result;
    }

    private void removeExpireData() {
        for (Map.Entry<String, Map<Integer, Pair<Map<HostPort, RedisMsg>, Date>>> entry : redisMsgCache.entrySet()) {
            List<Integer> expireIndex = new ArrayList<>();
            for (Map.Entry<Integer, Pair<Map<HostPort, RedisMsg>, Date>> dcEntry : entry.getValue().entrySet()) {
                if (new Date().getTime() - dcEntry.getValue().getValue().getTime() > config.getKeeperCheckerIntervalMilli() * 2L) {
                    expireIndex.add(dcEntry.getKey());
                }
            }
            for (int index : expireIndex) {
                logger.debug("[removeExpireData] remove expire index:{} time:{}, expire time:{}", index, redisMsgCache.get(entry.getKey()).get(index).getValue(), config.getKeeperCheckerIntervalMilli() * 2L);
                redisMsgCache.get(entry.getKey()).remove(index);
            }
        }
    }

    private Map<String, KeeperContainerUsedInfoModel> redisMsgMap2KeeperContainerUsedInfoModelsUtil(Map<HostPort, RedisMsg> redisMsgMap) {
        Map<String, KeeperContainerUsedInfoModel> result = new HashMap<>();
        for (DcMeta dcMeta : metaCache.getXpipeMeta().getDcs().values()) {
            if (!currentDc.equals(dcMeta.getId())) continue;
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    List<RedisTbl> allByDcClusterShard = null;
                    try {
                        allByDcClusterShard = redisService.findAllByDcClusterShard(dcMeta.getId(), clusterMeta.getId(), shardMeta.getId());
                    } catch (Throwable e){
                        logger.error("[redisMsgMap2KeeperContainerUsedInfoModelsUtil][{}-{}-{}] findAllByDcClusterShard error", dcMeta.getId(), clusterMeta.getId(), shardMeta.getId(), e);
                    }
                    if (shardMeta.getRedises() == null || shardMeta.getRedises().isEmpty()) continue;
                    RedisMsg redisMsg =  redisMsgMap.get(new HostPort(shardMeta.getRedises().get(0).getIp(), shardMeta.getRedises().get(0).getPort()));
                    for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
                        if (!result.containsKey(keeperMeta.getIp())) {
                            result.put(keeperMeta.getIp(), getKeeperBasicModel(keeperMeta.getIp(), dcMeta.getId()));
                        }
                        KeeperContainerUsedInfoModel model = result.get(keeperMeta.getIp());
                        DcClusterShardKeeper dcClusterShardKeeper = new DcClusterShardKeeper(dcMeta.getId(), clusterMeta.getId(), shardMeta.getId(), false, keeperMeta.getPort());
                        boolean keeperActive = false;
                        if (allByDcClusterShard != null) {
                            for (RedisTbl redisTbl : allByDcClusterShard) {
                                if (redisTbl.getRedisIp().equals(keeperMeta.getIp()) && redisTbl.getRedisPort() == keeperMeta.getPort()) {
                                    keeperActive = redisTbl.isKeeperActive();
                                    dcClusterShardKeeper.setActive(keeperActive);
                                    break;
                                }
                            }
                        }

                        KeeperContainerUsedInfoModel.KeeperUsedInfo keeperUsedInfo = new KeeperContainerUsedInfoModel.KeeperUsedInfo(redisMsg.getUsedMemory(), redisMsg.getInPutFlow(), keeperMeta.getIp());
                        model.getDetailInfo().put(dcClusterShardKeeper, keeperUsedInfo);
                        if (keeperActive) {
                            model.setActiveKeeperCount(model.getActiveKeeperCount() + 1)
                                    .setActiveInputFlow(model.getActiveInputFlow() + redisMsg.getInPutFlow())
                                    .setActiveRedisUsedMemory(model.getActiveRedisUsedMemory() + redisMsg.getUsedMemory());
                        }
                        model.setTotalKeeperCount(model.getTotalKeeperCount() + 1)
                                .setTotalInputFlow(model.getTotalInputFlow() + redisMsg.getInPutFlow())
                                .setTotalRedisUsedMemory(model.getTotalRedisUsedMemory() + redisMsg.getUsedMemory());

                    }
                }
            }
            dcMeta.getKeeperContainers().forEach(keeperContainerMeta -> {
                if (!result.containsKey(keeperContainerMeta.getIp())) {
                    result.put(keeperContainerMeta.getIp(),  getKeeperBasicModel(keeperContainerMeta.getIp(), dcMeta.getId()));
                }
            });
        }
        return result;
    }

    private KeeperContainerUsedInfoModel getKeeperBasicModel(String ip, String dcName) {
        KeeperContainerUsedInfoModel model = new KeeperContainerUsedInfoModel(ip, dcName, 0, 0);
        KeepercontainerTbl keepercontainerTbl = keeperContainerServiceImpl.find(ip);

        OrganizationTbl organizationTbl = organizationService.getOrganization(keepercontainerTbl.getKeepercontainerOrgId());
        if (organizationTbl != null) {
            model.setOrg(organizationTbl.getOrgName());
        }
        try {
            KeeperDiskInfo keeperDiskInfo = keeperContainerService.getKeeperDiskInfo(ip);
            if  (keeperDiskInfo != null && keeperDiskInfo.available && keeperDiskInfo.spaceUsageInfo != null) {
                model.setDiskAvailable(true)
                        .setDiskSize(keeperDiskInfo.spaceUsageInfo.size)
                        .setDiskUsed(keeperDiskInfo.spaceUsageInfo.use);
            } else {
                model.setDiskAvailable(false).setKeeperContainerHealth(false);
            }
        } catch (Throwable e){
            logger.error("[redisMsgMap2KeeperContainerUsedInfoModelsUtil] getKeeperDiskInfo error, keeperIp: {}", ip, e);
        }
        model.setDetailInfo(new HashMap<>())
                .setActiveKeeperCount(0)
                .setTotalKeeperCount(0)
                .setActiveInputFlow(0)
                .setActiveRedisUsedMemory(0)
                .setTotalInputFlow(0)
                .setTotalRedisUsedMemory(0);
        return model;
    }

    public void reportKeeperData(Map<String, KeeperContainerUsedInfoModel> modelMap) {
        for (Map.Entry<String, KeeperContainerUsedInfoModel> model : modelMap.entrySet()) {
            reportKeeperData(model.getValue(), DATA_TYPE, true);
            reportKeeperData(model.getValue(), DATA_TYPE, false);
            reportKeeperData(model.getValue(), TRAFFIC_TYPE, true);
            reportKeeperData(model.getValue(), TRAFFIC_TYPE, false);
        }
    }

    private void reportKeeperData(KeeperContainerUsedInfoModel model, String type, boolean active) {
        MetricData data = new MetricData(type);
        if (DATA_TYPE.equals(type)) {
            if (active) {
                data.setValue(model.getActiveRedisUsedMemory());
            } else {
                data.setValue(model.getTotalRedisUsedMemory() - model.getActiveRedisUsedMemory());
            }
        } else {
            if (active) {
                data.setValue(model.getActiveInputFlow());
            } else {
                data.setValue(model.getTotalInputFlow() - model.getActiveInputFlow());
            }
        }
        data.addTag("dc", model.getDcName());
        data.addTag("org", model.getOrg() == null ? "" : model.getOrg());
        data.addTag("ip", model.getKeeperIp());
        data.addTag("active", active ? "true" : "false");
        data.setTimestampMilli(System.currentTimeMillis());
        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Exception e) {
            logger.error("Error send metrics to metric", e);
        }
    }

    @PreDestroy
    public void destroy() {
        try {
            keeperContainerUsedInfoMsgCollectorTask.stop();
            this.scheduled.shutdownNow();
        } catch (Throwable th) {
            logger.info("[preDestroy] keeperContainerUsedInfoMsgCollectorTask destroy fail", th);
        }
    }

    @Override
    public void isleader() {
        try {
            logger.debug("[isleader] become leader");
            keeperContainerUsedInfoMsgCollectorTask.start();
        } catch (Throwable th) {
            logger.info("[isleader] keeperContainerUsedInfoMsgCollectorTask start fail", th);
        }
    }

    @Override
    public void notLeader() {
        try {
            logger.debug("[notLeader] loss leader");
            keeperContainerUsedInfoMsgCollectorTask.stop();
        } catch (Throwable th) {
            logger.info("[notLeader] keeperContainerUsedInfoMsgCollectorTask stop fail", th);
        }
    }
}
