package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
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
import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
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
public class KeeperContainerUsedInfoMsgCollector extends AbstractService implements ConsoleLeaderAware, CrossDcLeaderAware {

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

    @Autowired
    private CrossDcKeeperContainerAddressService keeperContainerAddressService;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask keeperContainerUsedInfoMsgCollectorTask;

    private ScheduledExecutorService extraSyncDCScheduled;

    private DynamicDelayPeriodTask extraSyncDCKeeperContainerUsedInfoMsgCollectorTask;

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
        this.keeperContainerUsedInfoMsgCollectorTask = new DynamicDelayPeriodTask("getAllCurrentDcRedisMsg", () -> getDcRedisMsgAndCalculate(currentDc), () -> config.getKeeperCheckerIntervalMilli(), scheduled);
        this.extraSyncDCScheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("ExtraSyncDCKeeperContainerUsedInfoMsgCollector"));
        this.extraSyncDCKeeperContainerUsedInfoMsgCollectorTask = new DynamicDelayPeriodTask("getExtraSyncDCRedisMsg", () -> consoleConfig.getExtraSyncDC().forEach(this::getDcRedisMsgAndCalculate), () -> config.getKeeperCheckerIntervalMilli(), extraSyncDCScheduled);
    }

    public void saveMsg(int index, Map<HostPort, RedisMsg> redisMsgMap) {
        reportCheckerReport(index);
        redisMasterMsgCache.put(index, new Pair<>(redisMsgMap, new Date()));
    }

    protected void getDcRedisMsgAndCalculate(String dcName) {
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

        Map<Long, KeeperContainerUsedInfoModel> modelMap = generateKeeperContainerUsedInfoModels(allRedisMasterMsg, dcName);
        reportKeeperData(modelMap);
        keeperContainerUsedInfoAnalyzer.updateKeeperContainerUsedInfo(dcName, modelMap);
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

    protected Map<Long, KeeperContainerUsedInfoModel> generateKeeperContainerUsedInfoModels(Map<HostPort, RedisMsg> redisMsgMap, String selectedDcName) {
        Map<Long, KeeperContainerUsedInfoModel> result = new HashMap<>();
        Map<String, Object> checkedCluster = new HashMap<>();
        for (DcMeta dcMeta : metaCache.getXpipeMeta().getDcs().values()) {
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                RedisMsg clusterAllRedisMsg = new RedisMsg();
                if (checkedCluster.containsKey(clusterMeta.getId())
                        || (!ClusterType.isSameClusterType(clusterMeta.getType(), ONE_WAY) && !ClusterType.isSameClusterType(clusterMeta.getType(), HETERO))) {
                    continue;
                }
                checkedCluster.put(clusterMeta.getId(), null);
                ClusterMeta selectedDcMeta = getDcMeta(clusterMeta, selectedDcName);
                if (selectedDcMeta == null) {
                    continue;
                }
                ClusterMeta masterDcMeta = dcMeta.getId().equals(clusterMeta.getActiveDc()) ? clusterMeta : metaCache.getXpipeMeta().findDc(clusterMeta.getActiveDc()).findCluster(clusterMeta.getId());
                for (ShardMeta shardMeta : masterDcMeta.getShards().values()) {
                    for(RedisMeta redisMeta : shardMeta.getRedises()){
                        if(redisMeta.isMaster()){
                            RedisMsg redisMsg = redisMsgMap.get(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
                            if (redisMsg != null) {
                                clusterAllRedisMsg.addRedisMsg(redisMsg);
                                ShardMeta currentDcShardMeta = selectedDcMeta.findShard(shardMeta.getId());
                                generateKeeperMsg(selectedDcName, clusterMeta.getId(), currentDcShardMeta.getId(), currentDcShardMeta.getKeepers(), result, redisMsg);
                            }
                        }
                    }
                }
                reportClusterData(clusterMeta.getId(), clusterAllRedisMsg, selectedDcName);
            }
            if (selectedDcName.equals(dcMeta.getId())) {
                dcMeta.getKeeperContainers().forEach(keeperContainerMeta -> {
                    Long keeperContainerId = keeperContainerMeta.getId();
                    if (!isValidKeeperContainerId(keeperContainerId)) {
                        logger.warn("[generateKeeperContainerUsedInfoModels] invalid keeperContainerId:{} for keeperContainer: ip={}, hostname={}",
                                keeperContainerId, keeperContainerMeta.getIp(), keeperContainerMeta.getHostname());
                        return;
                    }
                    if (!result.containsKey(keeperContainerId)) {
                        result.put(keeperContainerId, getKeeperBasicModel(keeperContainerId, keeperContainerMeta.getIp(), keeperContainerMeta.getHostname(), dcMeta.getId()));
                    }
                });
            }
        }
        return result;
    }

    private void generateKeeperMsg(String dc, String cluster, String shard, List<KeeperMeta> keeperMetas, Map<Long, KeeperContainerUsedInfoModel> result, RedisMsg redisMsg) {
        for (KeeperMeta keeperMeta : keeperMetas) {
            Long keeperContainerId = keeperMeta.getKeeperContainerId();
            if (!isValidKeeperContainerId(keeperContainerId)) {
                logger.warn("[generateKeeperMsg] invalid keeperContainerId:{} for keeper: {}-{}-{}, ip={}, hostname={}",
                        keeperContainerId, dc, cluster, shard, keeperMeta.getIp(), keeperMeta.getHostname());
                continue;
            }
            
            if (!result.containsKey(keeperContainerId)) {
                result.put(keeperContainerId, getKeeperBasicModel(keeperContainerId, keeperMeta.getIp(), keeperMeta.getHostname(), dc));
            }
            
            KeeperContainerUsedInfoModel model = result.get(keeperContainerId);
            DcClusterShardKeeper dcClusterShardKeeper = new DcClusterShardKeeper(dc, cluster, shard, keeperMeta.getActive(), keeperMeta.getPort());
            KeeperContainerUsedInfoModel.KeeperUsedInfo keeperUsedInfo = new KeeperContainerUsedInfoModel.KeeperUsedInfo(redisMsg.getUsedMemory(), redisMsg.getInPutFlow(), keeperMeta.getIp(), keeperContainerId);
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

    protected ClusterMeta getDcMeta(ClusterMeta clusterMeta, String dcName) {
        if (Objects.equals(clusterMeta.getActiveDc(), dcName)) {
            return metaCache.getXpipeMeta().findDc(dcName).findCluster(clusterMeta.getId());
        }
        if (clusterMeta.getBackupDcs() == null || clusterMeta.getBackupDcs().isEmpty()) {
            return null;
        }
        String[] split = clusterMeta.getBackupDcs().split(",");
        for (String dc : split) {
            if (Objects.equals(dc, dcName)) {
                return metaCache.getXpipeMeta().findDc(dcName).findCluster(clusterMeta.getId());
            }
        }
        return null;
    }

    /**
     * keeper 行的 keepercontainer_id 在 DB 里可为 NULL，经 DAL 的 primitive long 映射后读出来是 0，
     * 所以判空必须连 0 一起拦。否则这些"无主"keeper 会被聚合到 id=0 这个并不存在的容器上，
     * 流量与数据被累加成一个虚构的超载容器，进而产出无意义的迁移计划和资源不足告警。
     */
    private boolean isValidKeeperContainerId(Long keeperContainerId) {
        return keeperContainerId != null && keeperContainerId > 0;
    }

    private KeeperContainerUsedInfoModel getKeeperBasicModel(long keeperContainerId, String ip, String hostname, String dcName) {
        KeepercontainerTbl keepercontainerTbl = keeperContainerService.find(keeperContainerId);

        // keepercontainer 表才是容器身份的权威来源。传入的 ip/hostname 来自调用点（keeper 行或 keeperContainer 行），
        // 同一个容器可能先由 generateKeeperMsg 建、也可能先由 dcMeta.getKeeperContainers() 建，
        // 两边数据漂移时展示字段会变成"先到先得"，这里统一收敛到 DB 行，查不到才退回调用点给的值。
        String containerIp = keepercontainerTbl == null ? ip : keepercontainerTbl.getKeepercontainerIp();
        String containerHostname = keepercontainerTbl == null ? hostname : keepercontainerTbl.getKeepercontainerHostName();
        // 容器形态下登记的 ip 只是占位值，对外展示、指标上报、磁盘查询都要用反查到的实时 IP
        String resolvedIp = resolveIp(containerIp, containerHostname, dcName);

        KeeperContainerUsedInfoModel model = new KeeperContainerUsedInfoModel(resolvedIp, dcName, 0, 0);
        // keeperContainerId 是 model 的唯一标识（equals/hashCode 与整条 analyze 链路都依赖它），
        // 必须在任何提前返回之前落定，且只能来自 meta，不能靠 ip/hostname 反查 DB 得到，
        // 否则 DB 查不到时 id 为 null，会让 analyze 链路拿到无法定位的 model
        model.setKeeperContainerId(keeperContainerId)
                .setHostName(containerHostname)
                .setDetailInfo(new HashMap<>());

        if (keepercontainerTbl == null) {
            logger.warn("[getKeeperBasicModel] keepercontainer not found for id: {}, ip: {}, hostname: {}", keeperContainerId, ip, hostname);
            return model;
        }

        OrganizationTbl organizationTbl = organizationService.getOrganization(keepercontainerTbl.getKeepercontainerOrgId());
        if (organizationTbl != null) {
            model.setOrg(organizationTbl.getOrgName());
        }
        model.setTag(keepercontainerTbl.getTag());
        try {
            KeeperDiskInfo keeperDiskInfo = keeperContainerDiskInfoCollector.getKeeperDiskInfo(keeperContainerId);
            if (keeperDiskInfo == null && resolvedIp != null) {
                keeperDiskInfo = keeperContainerCheckerService.getKeeperDiskInfo(resolvedIp);
            }
            if  (keeperDiskInfo != null && keeperDiskInfo.available && keeperDiskInfo.spaceUsageInfo != null) {
                model.setDiskAvailable(true)
                        .setDiskSize(keeperDiskInfo.spaceUsageInfo.size)
                        .setDiskUsed(keeperDiskInfo.spaceUsageInfo.use);
            } else {
                model.setDiskAvailable(false).setKeeperContainerHealth(false);
            }
        } catch (Throwable th){
            logger.error("[KeeperContainerUsedInfoModel] getKeeperDiskInfo error, keeperContainerId: {}, keeperIp: {}", keeperContainerId, resolvedIp, th);
        }
        model.setActiveKeeperCount(0)
                .setTotalKeeperCount(0)
                .setActiveInputFlow(0)
                .setActiveRedisUsedMemory(0)
                .setTotalInputFlow(0)
                .setTotalRedisUsedMemory(0);
        return model;
    }

    /**
     * 物理机直接用登记的 IP；容器经 ZK 反查实时 IP。
     * <p>
     * 这里必须用跨机房版本：本 collector 除 currentDc 外还会为 extraSyncDC 生成数据，
     * 而 CurrentDcKeeperContainerAddressService 只 watch 本机房那套 ZK。
     * <p>
     * 反查不到时返回 null 而不是回退到占位 IP：占位值（DB 默认 0.0.0.0）在所有 pod 之间重复，
     * 拿它去发请求或上报指标只会产生互相串台的脏数据，宁可显式缺失。
     */
    private String resolveIp(String ip, String hostname, String dcName) {
        if (StringUtil.isEmpty(hostname)) {
            return ip;
        }
        String resolvedIp = keeperContainerAddressService.getIpByHostname(dcName, hostname);
        if (resolvedIp == null) {
            logger.warn("[resolveIp] failed to resolve hostname: {} in dc: {}", hostname, dcName);
        }
        return resolvedIp;
    }

    public void reportClusterData(String clusterName, RedisMsg clusterAllRedisMsg, String dcName) {
        reportClusterData(clusterName, CLUSTER_DATA_TYPE, clusterAllRedisMsg.getUsedMemory(), dcName);
        reportClusterData(clusterName, CLUSTER_TRAFFIC_TYPE, clusterAllRedisMsg.getInPutFlow(), dcName);
    }

    public void reportClusterData(String clusterName, String type, long value, String dcName) {
        MetricData data = new MetricData(type, dcName, clusterName, null);
        data.setValue(value);
        data.setTimestampMilli(System.currentTimeMillis());
        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Exception e) {
            logger.error("Error report keeper data to metric", e);
        }
    }

    public void reportKeeperData(Map<Long, KeeperContainerUsedInfoModel> modelMap) {
        for (Map.Entry<Long, KeeperContainerUsedInfoModel> model : modelMap.entrySet()) {
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
        // 容器反查不到实时 IP 时 keeperIp 为空，此时由 hostname 承担身份，不上报空 ip tag
        if (!StringUtil.isEmpty(model.getKeeperIp())) {
            data.addTag("ip", model.getKeeperIp());
        }
        // 容器形态下同时上报hostname,便于前端展示和问题定位
        if (!StringUtil.isEmpty(model.getHostName())) {
            data.addTag("hostname", model.getHostName());
        }
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
            extraSyncDCKeeperContainerUsedInfoMsgCollectorTask.stop();
            this.extraSyncDCScheduled.shutdownNow();
        } catch (Throwable th) {
            logger.info("[preDestroy] keeperContainerUsedInfoMsgCollectorTask destroy fail", th);
        }
    }

    @Override
    public void isleader() {
        try {
            logger.debug("[isleader] become leader");
            if (!consoleConfig.getExtraSyncDC().contains(currentDc.toUpperCase())) {
                keeperContainerUsedInfoMsgCollectorTask.start();
            }
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

    @Override
    public void isCrossDcLeader() {
        try {
            logger.debug("[isCrossDcLeader] become cross dc leader");
            extraSyncDCKeeperContainerUsedInfoMsgCollectorTask.start();
        } catch (Throwable th) {
            logger.info("[isCrossDcLeader] extraSyncDCKeeperContainerUsedInfoMsgCollectorTask start fail", th);
        }
    }

    @Override
    public void notCrossDcLeader() {
        try {
            logger.debug("[isCrossDcLeader] loss cross dc leader");
            extraSyncDCKeeperContainerUsedInfoMsgCollectorTask.stop();
        } catch (Throwable th) {
            logger.info("[isCrossDcLeader] extraSyncDCKeeperContainerUsedInfoMsgCollectorTask stop fail", th);
        }
    }
}
