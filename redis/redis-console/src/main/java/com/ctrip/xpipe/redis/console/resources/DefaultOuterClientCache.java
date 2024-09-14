package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.checker.OuterClientCache;
import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import com.ctrip.xpipe.api.migration.OuterClientService.*;

/**
 * @author lishanglin
 * date 2022/7/18
 * only cache current dc active dc one-way clusters
 */
@Component
public class DefaultOuterClientCache extends AbstractLifecycle implements OuterClientCache, TopElement {

    private OuterClientService outerClientService;

    private ConsoleConfig config;

    private TimeBoundCache<Map<String, ClusterInfo>> clustersCache;

    private TimeBoundCache<Map<String, ClusterInfo>> currentDcClustersCache;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask refreshTask;

    private DynamicDelayPeriodTask refreshCurrentDcTask;

    public DefaultOuterClientCache(ConsoleConfig config) {
        this.outerClientService = OuterClientService.DEFAULT;
        this.config = config;
        this.clustersCache = new TimeBoundCache<>(() -> 10000 + config.getRedisConfCheckIntervalMilli(),
                () -> loadClusters(FoundationService.DEFAULT.getDataCenter()));
        this.currentDcClustersCache = new TimeBoundCache<>(() -> 10000 + config.getRedisConfCheckIntervalMilli(),
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
            DcMeta currentDcClusterInfos = outerClientService.getOutClientDcMeta(dc);
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

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        this.scheduled = Executors.newScheduledThreadPool(1,
                XpipeThreadFactory.create("OuterClientCacheRefreshScheduled"));
        this.refreshTask = new DynamicDelayPeriodTask("OuterClientCacheRefresh", clustersCache::refresh,
                config::getRedisConfCheckIntervalMilli, scheduled);
        this.refreshCurrentDcTask = new DynamicDelayPeriodTask("OuterClientCurrentDcCacheRefresh", currentDcClustersCache::refresh,
                config::getRedisConfCheckIntervalMilli, scheduled);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        this.refreshTask.start();
        this.refreshCurrentDcTask.start();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        this.refreshTask.stop();
        this.refreshCurrentDcTask.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        this.scheduled.shutdown();
        this.scheduled = null;
        this.refreshTask = null;
        this.refreshCurrentDcTask = null;
    }

}
