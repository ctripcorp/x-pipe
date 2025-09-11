package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.migration.OuterClientService.*;
import com.ctrip.xpipe.redis.checker.OuterClientCache;
import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author lishanglin
 * date 2022/7/18
 * only cache current dc active dc one-way clusters
 */
@Component
public class DefaultOuterClientCache implements ConsoleLeaderAware, OuterClientCache {

    private OuterClientService outerClientService;

    private ConsoleConfig config;

    private TimeBoundCache<Map<String, ClusterInfo>> clustersCache;

    private TimeBoundCache<Map<String, ClusterInfo>> currentDcClustersCache;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1,
            XpipeThreadFactory.create("OuterClientCacheRefreshScheduled"));;

    private DynamicDelayPeriodTask refreshTask;

    private DynamicDelayPeriodTask refreshCurrentDcTask;

    protected Logger logger = LoggerFactory.getLogger(getClass());


    public DefaultOuterClientCache(ConsoleConfig config) {
        this.outerClientService = OuterClientService.DEFAULT;
        this.config = config;
        this.clustersCache = new TimeBoundCache<>(() -> 10000 + getIntervalMilli(),
                () -> loadClusters(FoundationService.DEFAULT.getDataCenter()));
        this.currentDcClustersCache = new TimeBoundCache<>(() -> 10000 + getIntervalMilli(),
                () -> loadCurrentDcClusters(FoundationService.DEFAULT.getDataCenter()));
    }

    @Override
    public ClusterInfo getClusterInfo(String clusterName) throws Exception {
        ClusterInfo clusterInfo = clustersCache.getData(false).get(clusterName.toLowerCase());
        if (null == clusterInfo) {
            return outerClientService.getClusterInfo(clusterName);
        }

        return clusterInfo;
    }

    @Override
    public Map<String, ClusterInfo> getAllDcClusters(String dc) {
        if (FoundationService.DEFAULT.getDataCenter().equalsIgnoreCase(dc)) return clustersCache.getData(false);
        else return loadClusters(dc);
    }

    @Override
    public Map<String, ClusterInfo> getAllCurrentDcClusters(String dc) {
        if (FoundationService.DEFAULT.getDataCenter().equalsIgnoreCase(dc)) return currentDcClustersCache.getData(false);
        else return loadCurrentDcClusters(dc);
    }

    private Map<String, ClusterInfo> loadClusters(String dc) {
        Map<String, ClusterInfo> clusters = new HashMap<>();
        try {
            List<ClusterInfo> clusterInfos = outerClientService.getActiveDcClusters(dc);
            for (ClusterInfo cluster: clusterInfos) {
                clusters.put(cluster.getName().toLowerCase(), cluster);
            }
        } catch (RestClientException e) {
            logger.warn("[refresh] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[refresh] fail", th);
        }

        return clusters;
    }

    private Map<String, ClusterInfo> loadCurrentDcClusters(String dc) {
        Map<String, ClusterInfo> clusters = new HashMap<>();
        try {
            OuterClientService.DcMeta currentDcClusterInfos = outerClientService.getOutClientDcMeta(dc);
            for (ClusterMeta clusterMeta: currentDcClusterInfos.getClusters().values()) {
                if (!ClusterType.XPIPE_ONE_WAY.equals(clusterMeta.getClusterType())) continue;
                ClusterInfo clusterInfo = buildClusterModel(clusterMeta);
                if (!clusters.containsKey(clusterInfo.getName().toLowerCase())) {
                    clusters.put(clusterInfo.getName().toLowerCase(), clusterInfo);
                }
            }
        } catch (RestClientException e) {
            logger.warn("[refresh] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[refresh] fail", th);
        }

        return clusters;
    }

    public ClusterInfo buildClusterModel(ClusterMeta clusterMeta) {
        ClusterInfo cluster = clusterMetaToClusterModel(clusterMeta);
        List<GroupInfo> groupModels = new ArrayList<>();
        for (GroupMeta group : clusterMeta.getGroups().values()) {
            List<InstanceInfo> instanceModels = new ArrayList<>();
            GroupInfo groupModel = groupMetaToGroupModel(group);
            for (RedisMeta instance : group.getRedises()) {
                instanceModels.add(redisMetaToInstanceModel(instance));
            }
            groupModel.setInstances(instanceModels);
            groupModels.add(groupModel);
        }
        cluster.setGroups(groupModels);
        return cluster;
    }

    public ClusterInfo clusterMetaToClusterModel(ClusterMeta clusterMeta) {
        ClusterInfo cluster = new ClusterInfo();
        cluster.setName(clusterMeta.getName());
        if (clusterMeta.getClusterType().equals(ClusterType.XPIPE_ONE_WAY)) {
            cluster.setIsXpipe(true);
            cluster.setMasterIDC(clusterMeta.getActiveIDC());
        }
        return cluster;
    }

    public GroupInfo groupMetaToGroupModel(GroupMeta groupMeta) {
        GroupInfo groupModel = new GroupInfo();
        groupModel.setName(groupMeta.getGroupName());
        return groupModel;
    }

    public InstanceInfo redisMetaToInstanceModel(RedisMeta redisMeta) {
        InstanceInfo instance = new InstanceInfo();
        instance.setIPAddress(redisMeta.getHost());
        instance.setPort(redisMeta.getPort());
        instance.setIsMaster(redisMeta.isMaster());
        instance.setStatus(InstanceStatus.ACTIVE.equals(redisMeta.getStatus()));
        instance.setEnv(redisMeta.getIdc());
        instance.setCanRead(!InstanceStatus.INACTIVE.equals(redisMeta.getStatus()));
        return instance;
    }

    private synchronized void stopLoadData() throws Exception {

        logger.info("[refreshCurrentDcTask][stop]");
        if(refreshCurrentDcTask != null){
            refreshCurrentDcTask.stop();
        }
        refreshCurrentDcTask = null;
        logger.info("[refreshTask][stop]");
        if(refreshTask != null){
            refreshTask.stop();
        }
        refreshTask = null;

    }

    private synchronized void startLoadData() throws Exception {

        if(refreshCurrentDcTask == null){
            this.refreshCurrentDcTask = new DynamicDelayPeriodTask("OuterClientCurrentDcCacheRefresh", currentDcClustersCache::refresh,
                    this::getIntervalMilli, scheduled);
        }

        this.refreshCurrentDcTask.start();
        logger.info("[refreshCurrentDcTask][start]");

        if(refreshTask == null){
            this.refreshTask = new DynamicDelayPeriodTask("OuterClientCacheRefresh", clustersCache::refresh,
                    this::getIntervalMilli, scheduled);
        }

        this.refreshTask.start();

        logger.info("[refreshTask][start]");
    }

    private void doStart() {
        logger.info("[doStart]");
        try {
            startLoadData();
        } catch (Throwable th) {
            logger.error("[doStart]", th);
        }
    }

    private void doStop() {
        try {
            stopLoadData();
        } catch (Throwable th) {
            logger.error("[doStop]", th);
        }
    }

    @Override
    public void isleader() {
        doStop();
        doStart();
    }

    @Override
    public void notLeader() {
        doStop();
    }

    private int getIntervalMilli() {
        return config.getCRedisClusterCacheRefreshIntervalMilli();
    }
}
