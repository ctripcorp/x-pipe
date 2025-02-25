package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.cluster.ClusterType;
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
import com.ctrip.xpipe.redis.console.keeper.entity.CheckerReportSituation;
import com.ctrip.xpipe.redis.console.keeper.entity.DcCheckerReportMsg;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.cluster.ClusterType.HETERO;
import static com.ctrip.xpipe.cluster.ClusterType.ONE_WAY;

@Component
public class KeeperContainerUsedInfoMsgCollector extends AbstractService implements ConsoleLeaderAware {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private KeeperContainerCheckerService keeperContainerCheckerService;

    @Autowired
    private KeeperContainerService keeperContainerService;

    @Autowired
    private OrganizationService organizationService;

    @Autowired
    private CheckerConfig config;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private KeeperContainerUsedInfoAnalyzer keeperContainerUsedInfoAnalyzer;

    @Autowired
    private KeeperContainerDiskInfoCollector keeperContainerDiskInfoCollector;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask keeperContainerUsedInfoMsgCollectorTask;

    protected Map<Integer, Pair<Map<HostPort, RedisMsg>, Date>> redisMasterMsgCache = new ConcurrentHashMap<>();

    private Map<String, CheckerReportSituation> dcCheckerReportSituationMap = new HashMap<>();

    protected MetricProxy metricProxy = MetricProxy.DEFAULT;

    private static final String CLUSTER_TRAFFIC_TYPE = "cluster.traffic";

    private static final String CLUSTER_DATA_TYPE = "cluster.data";

    private static final String TRAFFIC_TYPE = "keepercontainer.traffic";

    private static final String DATA_TYPE = "keepercontainer.data";

    private static final String CHECKER_REPORT_TYPE = "keepercontainer.checker.report";

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();
    
    private static final Logger logger = LoggerFactory.getLogger(KeeperContainerUsedInfoMsgCollector.class);

    @PostConstruct
    public void init() {
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("KeeperContainerUsedInfoMsgCollector"));
        this.keeperContainerUsedInfoMsgCollectorTask = new DynamicDelayPeriodTask("getAllCurrentDcRedisMsg",
                this::getAllCurrentDcRedisMsgAndCalculate, () -> config.getKeeperCheckerIntervalMilli(), scheduled);
    }

    public void saveMsg(int index, Map<HostPort, RedisMsg> redisMsgMap) {
        reportCheckerReport(index);
        redisMasterMsgCache.put(index, new Pair<>(redisMsgMap, new Date()));
    }

    protected void getAllCurrentDcRedisMsgAndCalculate() {
        Map<HostPort, RedisMsg> allRedisMasterMsg = new HashMap<>();
        Map<String, CheckerReportSituation> tmpDcCheckerReportSituationMap = new HashMap<>();
        for (Map.Entry<String, String> entry : consoleConfig.getConsoleDomains().entrySet()) {
            DcCheckerReportMsg dcRedisMsg = null;
            if (currentDc.equals(entry.getKey())) {
                dcRedisMsg = getCurrentDcRedisMasterMsg();
            } else {
                ResponseEntity<DcCheckerReportMsg> response = restTemplate.exchange(entry.getValue() + "/api/keepercontainer/redis/msg", HttpMethod.GET, null, DcCheckerReportMsg.class);
                if (response.getBody() != null) {
                    dcRedisMsg = response.getBody();
                }
            }
            if (dcRedisMsg != null) {
                allRedisMasterMsg.putAll(dcRedisMsg.getRedisMsg());
                tmpDcCheckerReportSituationMap.put(entry.getKey(), dcRedisMsg.getCheckerReportSituation());
            }
        }
        dcCheckerReportSituationMap = tmpDcCheckerReportSituationMap;
        if (allRedisMasterMsg.isEmpty()) return;

        Map<String, KeeperContainerUsedInfoModel> modelMap = generateKeeperContainerUsedInfoModels(allRedisMasterMsg);
        reportKeeperData(modelMap);
        keeperContainerUsedInfoAnalyzer.updateKeeperContainerUsedInfo(modelMap);
    }

    public DcCheckerReportMsg getCurrentDcRedisMasterMsg() {
        removeExpireData();
        Map<HostPort, RedisMsg> allRedisMsg = new HashMap<>();
        List<Integer> indexList = new ArrayList<>();
        DcCheckerReportMsg result = new DcCheckerReportMsg(allRedisMsg, currentDc, indexList, consoleConfig.getClusterDividedParts());
        for (Map.Entry<Integer, Pair<Map<HostPort, RedisMsg>, Date>> entry : redisMasterMsgCache.entrySet()) {
            allRedisMsg.putAll(entry.getValue().getKey());
            indexList.add(entry.getKey());
        }
        Collections.sort(indexList);
        return result;
    }

    private void removeExpireData() {
        for (Map.Entry<Integer, Pair<Map<HostPort, RedisMsg>, Date>> entry : redisMasterMsgCache.entrySet()) {
            List<Integer> expireIndex = new ArrayList<>();
            if (new Date().getTime() - entry.getValue().getValue().getTime() > config.getKeeperCheckerIntervalMilli() * 2L) {
                expireIndex.add(entry.getKey());
            }
            for (int index : expireIndex) {
                logger.debug("[removeExpireData] remove expire index:{} time:{}, expire time:{}", index, redisMasterMsgCache.get(index).getValue(), config.getKeeperCheckerIntervalMilli() * 2L);
                redisMasterMsgCache.remove(index);
            }
        }
    }

    protected Map<String, KeeperContainerUsedInfoModel> generateKeeperContainerUsedInfoModels(Map<HostPort, RedisMsg> redisMsgMap) {
        Map<String, KeeperContainerUsedInfoModel> result = new HashMap<>();
        Map<String, Object> checkedCluster = new HashMap<>();
        for (DcMeta dcMeta : metaCache.getXpipeMeta().getDcs().values()) {
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                RedisMsg clusterAllRedisMsg = new RedisMsg();
                if (checkedCluster.containsKey(clusterMeta.getId())
                        || (!ClusterType.isSameClusterType(clusterMeta.getType(), ONE_WAY) && !ClusterType.isSameClusterType(clusterMeta.getType(), HETERO))) {
                    continue;
                }
                checkedCluster.put(clusterMeta.getId(), null);
                ClusterMeta currentDcMeta = getCurrentDcMeta(clusterMeta);
                if (currentDcMeta == null) {
                    continue;
                }
                ClusterMeta masterDcMeta = dcMeta.getId().equals(clusterMeta.getActiveDc()) ? clusterMeta : metaCache.getXpipeMeta().findDc(clusterMeta.getActiveDc()).findCluster(clusterMeta.getId());
                for (ShardMeta shardMeta : masterDcMeta.getShards().values()) {
                    for(RedisMeta redisMeta : shardMeta.getRedises()){
                        if(redisMeta.isMaster()){
                            RedisMsg redisMsg = redisMsgMap.get(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
                            if (redisMsg != null) {
                                clusterAllRedisMsg.addRedisMsg(redisMsg);
                                ShardMeta currentDcShardMeta = currentDcMeta.findShard(shardMeta.getId());
                                generateKeeperMsg(currentDc, clusterMeta.getId(), currentDcShardMeta.getId(), currentDcShardMeta.getKeepers(), result, redisMsg);
                            }
                        }
                    }
                }
//                reportClusterData(clusterMeta.getId(), clusterAllRedisMsg);
            }
            if (currentDc.equals(dcMeta.getId())) {
                dcMeta.getKeeperContainers().forEach(keeperContainerMeta -> {
                    if (!result.containsKey(keeperContainerMeta.getIp())) {
                        result.put(keeperContainerMeta.getIp(),  getKeeperBasicModel(keeperContainerMeta.getIp(), dcMeta.getId()));
                    }
                });
            }
        }
        return result;
    }

    private void generateKeeperMsg(String dc, String cluster, String shard, List<KeeperMeta> keeperMetas, Map<String, KeeperContainerUsedInfoModel> result, RedisMsg redisMsg) {
        for (KeeperMeta keeperMeta : keeperMetas) {
            if (!result.containsKey(keeperMeta.getIp())) {
                result.put(keeperMeta.getIp(), getKeeperBasicModel(keeperMeta.getIp(), dc));
            }
            KeeperContainerUsedInfoModel model = result.get(keeperMeta.getIp());
            DcClusterShardKeeper dcClusterShardKeeper = new DcClusterShardKeeper(dc, cluster, shard, keeperMeta.getActive(), keeperMeta.getPort());
            KeeperContainerUsedInfoModel.KeeperUsedInfo keeperUsedInfo = new KeeperContainerUsedInfoModel.KeeperUsedInfo(redisMsg.getUsedMemory(), redisMsg.getInPutFlow(), keeperMeta.getIp());
            model.getDetailInfo().put(dcClusterShardKeeper, keeperUsedInfo);
            if (dcClusterShardKeeper.isActive()) {
                model.setActiveKeeperCount(model.getActiveKeeperCount() + 1)
                        .setActiveInputFlow(model.getActiveInputFlow() + redisMsg.getInPutFlow())
                        .setActiveRedisUsedMemory(model.getActiveRedisUsedMemory() + redisMsg.getUsedMemory());
            }
            model.setTotalKeeperCount(model.getTotalKeeperCount() + 1)
                    .setTotalInputFlow(model.getTotalInputFlow() + redisMsg.getInPutFlow())
                    .setTotalRedisUsedMemory(model.getTotalRedisUsedMemory() + redisMsg.getUsedMemory())
                    .setUpdateTime(DateTimeUtils.currentTimeAsString());

        }
    }

    protected ClusterMeta getCurrentDcMeta(ClusterMeta clusterMeta) {
        if (Objects.equals(clusterMeta.getActiveDc(), currentDc)) {
            return metaCache.getXpipeMeta().findDc(currentDc).findCluster(clusterMeta.getId());
        }
        if (clusterMeta.getBackupDcs() == null || clusterMeta.getBackupDcs().isEmpty()) {
            return null;
        }
        String[] split = clusterMeta.getBackupDcs().split(",");
        for (String dc : split) {
            if (Objects.equals(dc, currentDc)) {
                return metaCache.getXpipeMeta().findDc(currentDc).findCluster(clusterMeta.getId());
            }
        }
        return null;
    }

    private KeeperContainerUsedInfoModel getKeeperBasicModel(String ip, String dcName) {
        KeeperContainerUsedInfoModel model = new KeeperContainerUsedInfoModel(ip, dcName, 0, 0);
        KeepercontainerTbl keepercontainerTbl = keeperContainerService.find(ip);

        OrganizationTbl organizationTbl = organizationService.getOrganization(keepercontainerTbl.getKeepercontainerOrgId());
        if (organizationTbl != null) {
            model.setOrg(organizationTbl.getOrgName());
        }
        try {
            KeeperDiskInfo keeperDiskInfo = keeperContainerDiskInfoCollector.getKeeperDiskInfo(ip);
            if (keeperDiskInfo == null) {
                keeperDiskInfo = keeperContainerCheckerService.getKeeperDiskInfo(ip);
            }
            if  (keeperDiskInfo != null && keeperDiskInfo.available && keeperDiskInfo.spaceUsageInfo != null) {
                model.setDiskAvailable(true)
                        .setDiskSize(keeperDiskInfo.spaceUsageInfo.size)
                        .setDiskUsed(keeperDiskInfo.spaceUsageInfo.use);
            } else {
                model.setDiskAvailable(false).setKeeperContainerHealth(false);
            }
        } catch (Throwable th){
            logger.error("[KeeperContainerUsedInfoModel] getKeeperDiskInfo error, keeperIp: {}", ip, th);
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

    public void reportClusterData(String clusterName, RedisMsg clusterAllRedisMsg) {
        reportClusterData(clusterName, CLUSTER_DATA_TYPE, clusterAllRedisMsg.getUsedMemory());
        reportClusterData(clusterName, CLUSTER_TRAFFIC_TYPE, clusterAllRedisMsg.getInPutFlow());
    }

    public void reportClusterData(String clusterName, String type, long value) {
        MetricData data = new MetricData(type, currentDc, clusterName, null);
        data.setValue(value);
        data.setTimestampMilli(System.currentTimeMillis());
        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Exception e) {
            logger.error("Error report keeper data to metric", e);
        }
    }

    public void reportKeeperData(Map<String, KeeperContainerUsedInfoModel> modelMap) {
        for (Map.Entry<String, KeeperContainerUsedInfoModel> model : modelMap.entrySet()) {
            reportKeeperData(model.getValue(), DATA_TYPE, true);
            reportKeeperData(model.getValue(), DATA_TYPE, false);
            reportKeeperData(model.getValue(), TRAFFIC_TYPE, true);
            reportKeeperData(model.getValue(), TRAFFIC_TYPE, false);
        }
    }

    protected void reportKeeperData(KeeperContainerUsedInfoModel model, String type, boolean active) {
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
        data.addTag("org", model.getOrg() == null ? "common" : model.getOrg());
        data.addTag("ip", model.getKeeperIp());
        data.addTag("active", active ? "1" : "0");
        data.setTimestampMilli(System.currentTimeMillis());
        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Exception e) {
            logger.error("Error report keeper data to metric", e);
        }
    }

    protected void reportCheckerReport(int index) {
        MetricData data = new MetricData(CHECKER_REPORT_TYPE);
        data.addTag("dc", currentDc);
        data.addTag("index", String.valueOf(index));
        data.setValue(1);
        data.setTimestampMilli(System.currentTimeMillis());
        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Exception e) {
            logger.error("Error report checker report to metric", e);
        }
    }

    public Map<String, CheckerReportSituation> getDcCheckerReportSituationMap() {
        return dcCheckerReportSituationMap;
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
