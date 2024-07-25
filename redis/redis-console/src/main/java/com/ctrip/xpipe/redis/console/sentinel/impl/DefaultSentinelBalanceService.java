package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.SentinelGroupService;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.console.service.impl.ClusterServiceImpl.CLUSTER_DEFAULT_TAG;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author lishanglin
 * date 2021/8/31
 */
@Service
public class DefaultSentinelBalanceService implements SentinelBalanceService {

    private SentinelGroupService sentinelService;

    private DcClusterShardService dcClusterShardService;

    private ScheduledExecutorService balanceScheduled;

    private ConsoleConfig config;

    private ExecutorService executors;

    private TimeBoundCache<Map<String, SentinelsCache>> cachedSentinels;

    private Map<String, SentinelBalanceTasks> balanceTasks = Maps.newConcurrentMap();

    private static final Logger logger = LoggerFactory.getLogger(DefaultSentinelBalanceService.class);

    @Autowired
    public DefaultSentinelBalanceService(SentinelGroupService sentinelService, DcClusterShardService dcClusterShardService,
                                         ConsoleConfig config, @Qualifier(GLOBAL_EXECUTOR) ExecutorService executors) {
        this.sentinelService = sentinelService;
        this.dcClusterShardService = dcClusterShardService;
        this.config = config;
        this.balanceScheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("SentinelBalanceScheduled"));
        this.executors = executors;
        this.cachedSentinels = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::refreshCache);
    }

    @Override
    public List<SentinelGroupModel> getCachedDcSentinel(String dcId, ClusterType clusterType, String tag) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);
        if (StringUtil.isEmpty(dcId) || !sentinels.containsKey(clusterType.name().toUpperCase())) {
            return Collections.emptyList();
        }

        SentinelsCache typeSentinelCache = sentinels.get(clusterType.name().toUpperCase());
        DcSentinels dcSentinels = typeSentinelCache.getByDc(dcId.toUpperCase());
        if (dcSentinels == null)
            return Collections.emptyList();
        List<SentinelGroupModel> result = new ArrayList<>();
        if (tag == null) tag = CLUSTER_DEFAULT_TAG;
        for (SentinelGroupModel sentinel : dcSentinels.getSentinels()) {
            if (StringUtil.trimEquals(tag, sentinel.getTag(), true)) {
                result.add(sentinel);
            }
        }
        return result;
    }

    @Override
    public SentinelGroupModel selectSentinel(String dcId, ClusterType clusterType, String tag) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);
        if (StringUtil.isEmpty(dcId) || !sentinels.containsKey(clusterType.name().toUpperCase())) {
            return null;
        }

        DcSentinels dcSentinels = sentinels.get(clusterType.name().toUpperCase()).getByDc(dcId.toUpperCase());
        if (dcSentinels == null)
            return null;

        return selectSentinel(dcSentinels, tag);
    }

    @Override
    public SentinelGroupModel selectSentinelWithoutCache(String dcId, ClusterType clusterType, String tag) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(true);
        if (StringUtil.isEmpty(dcId)) {
            return null;
        }

        SentinelsCache sentinelsCache = sentinels.get(clusterType.name().toUpperCase());
        if (sentinelsCache == null)
            return null;

        DcSentinels dcSentinels = sentinelsCache.getByDc(dcId.toUpperCase());
        if (dcSentinels == null)
            return null;

        return selectSentinel(dcSentinels, tag);
    }

    @Override
    public Map<Long, SentinelGroupModel> selectMultiDcSentinels(ClusterType clusterType, String tag) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);

        Map<Long, SentinelGroupModel> sentinelMap = new HashMap<>();
        SentinelsCache typeSentinelsCache = sentinels.get(clusterType.name().toUpperCase());
        if (typeSentinelsCache == null) {
            return sentinelMap;
        }

        for (DcSentinels dcSentinels: typeSentinelsCache.getDcSentinelsMap().values()) {
            sentinelMap.put(dcSentinels.getDcId(), selectSentinel(dcSentinels, tag));
        }
        return sentinelMap;
    }

    private SentinelGroupModel selectSentinel(DcSentinels dcSentinels, String tag) {
        if (dcSentinels == null)
            return null;
        List<SentinelGroupModel> sentinels = dcSentinels.getSentinels();
        long minCnt = Long.MAX_VALUE;
        SentinelGroupModel idealSentinel = null;
        for (SentinelGroupModel sentinel: sentinels) {
            if (sentinel.isActive() && StringUtil.trimEquals(tag, sentinel.getTag(), true) && sentinel.getShardCount() < minCnt) {
                minCnt = sentinel.getShardCount();
                idealSentinel = sentinel;
            }
        }

        return idealSentinel;
    }

    @Override
    public synchronized void rebalanceDcSentinel(String dc, ClusterType clusterType, String tag) {
        checkCurrentTask(dc, clusterType, tag);

        SentinelBalanceTask task = new AllSentinelsBalanceTask(dc, tag, this, dcClusterShardService, balanceScheduled,
                config.getRebalanceSentinelMaxNumOnce(), config.getRebalanceSentinelInterval());
        balanceTasks.putIfAbsent(clusterType.name().toUpperCase(), new SentinelBalanceTasks());
        balanceTasks.get(clusterType.name().toUpperCase()).addTasks(dc.toUpperCase(), tag.toUpperCase(), task);
        task.execute(executors);
    }

    @Override
    public synchronized void rebalanceBackupDcSentinel(String dc, String tag) {
        checkCurrentTask(dc, ClusterType.ONE_WAY, tag);

        SentinelBalanceTask task = new BackupDcOnlySentinelBalanceTask(dc, this, dcClusterShardService, tag);

        balanceTasks.putIfAbsent(ClusterType.ONE_WAY.name().toUpperCase(), new SentinelBalanceTasks());
        balanceTasks.get(ClusterType.ONE_WAY.name().toUpperCase()).addTasks(dc.toUpperCase(), tag.toUpperCase(), task);

        task.execute(executors);
    }

    @Override
    public synchronized void cancelCurrentBalance(String dc, ClusterType clusterType, String tag) {
        if (StringUtil.isEmpty(dc)) throw new IllegalArgumentException("unexpected dc " + dc);
        if (clusterType == null) throw new IllegalArgumentException("sentinel type cannot be null");

        if(cachedSentinels.getData(false).get(clusterType.name().toUpperCase())==null) throw new IllegalArgumentException("unexpected cluster type " + clusterType.name());

        SentinelBalanceTasks tasks = balanceTasks.get(clusterType.name().toUpperCase());
        if (tasks == null)
            return;
        SentinelBalanceTask task = tasks.getTaskByDcTag(dc.toUpperCase(), tag.toUpperCase());
        if (task != null) {
            task.future().cancel(true);
            tasks.removeTask(dc.toUpperCase(), tag.toUpperCase());
        }
    }

    private void checkCurrentTask(String dc, ClusterType clusterType, String tag) {
        if (StringUtil.isEmpty(dc)) throw new IllegalArgumentException("unexpected dc " + dc);

        if(cachedSentinels.getData(false).get(clusterType.name().toUpperCase())==null) throw new IllegalArgumentException("unexpected cluster type " + clusterType.name());

        SentinelBalanceTasks typeTasks = balanceTasks.get(clusterType.name().toUpperCase());
        if (typeTasks == null)
            return;

        Map<String, SentinelBalanceTask> dcTagTask = typeTasks.getTasks().get(dc.toUpperCase());
        if (dcTagTask != null) {
            SentinelBalanceTask tagTask = dcTagTask.get(tag.toUpperCase());
            if (tagTask != null) {
                if (!tagTask.future().isDone()) {
                    throw new IllegalStateException("current task running");
                }
            }
        }
    }

    @Override
    public SentinelBalanceTask getBalanceTask(String dcId, ClusterType clusterType, String tag) {

        SentinelBalanceTasks typeTasks = balanceTasks.get(clusterType.name().toUpperCase());
        if (StringUtil.isEmpty(dcId) || typeTasks == null) {
            return null;
        }

        return typeTasks.getTaskByDcTag(dcId.toUpperCase(), tag.toUpperCase());
    }

    private Map<String, SentinelsCache> refreshCache() {
        logger.debug("[refreshCache] begin");
        List<SentinelGroupModel> sentinelGroups = sentinelService.getAllSentinelGroupsWithUsage();

        Map<String, List<SentinelGroupModel>> sentinelsCollectedByType = sentinelGroups.stream().collect(
                Collectors.toMap(SentinelGroupModel::getClusterType,
                        Lists::newArrayList, (v1, v2) -> {
                            v1.addAll(v2);
                            return v1;
                        }));
        Map<String, SentinelsCache> result = new HashMap<>();
        sentinelsCollectedByType.forEach((k, v) -> {
            Map<String, DcSentinels> newCacheData = new HashMap<>();
            for (SentinelGroupModel sentinelGroup : v) {
                if (sentinelGroup.getSentinels().isEmpty())
                    continue;
                Map<String, Long> dcInfos = sentinelGroup.dcInfos();
                for (String dcName : dcInfos.keySet()) {
                    newCacheData.putIfAbsent(dcName.toUpperCase(), new DcSentinels(dcInfos.get(dcName)));
                    newCacheData.get(dcName.toUpperCase()).addSentinel(sentinelGroup);
                }
            }
            result.put(k.toUpperCase(), new SentinelsCache(newCacheData));
        });

        return result;
    }

    private static class SentinelsCache {
        private Map<String, DcSentinels> dcSentinelsMap;

        public SentinelsCache(Map<String, DcSentinels> dcSentinelsMap) {
            this.dcSentinelsMap = dcSentinelsMap;
        }

        public Map<String, DcSentinels> getDcSentinelsMap() {
            return dcSentinelsMap;
        }

        public void setDcSentinelsMap(Map<String, DcSentinels> dcSentinelsMap) {
            this.dcSentinelsMap = dcSentinelsMap;
        }

        public DcSentinels getByDc(String dc){
            return dcSentinelsMap.get(dc);
        }

        public List<SentinelGroupModel> getAllSentinelGroups() {
            Map<Long, SentinelGroupModel> allGroups = new HashMap<>();
            dcSentinelsMap.values().forEach(dcSentinels -> {
                dcSentinels.getSentinels().forEach(sentinelGroupModel -> {
                    allGroups.put(sentinelGroupModel.getSentinelGroupId(), sentinelGroupModel);
                });
            });
            return new ArrayList<>(allGroups.values());
        }
    }

    private static class SentinelBalanceTasks {
        private Map<String, Map<String, SentinelBalanceTask>> tasks = new HashMap<>();

        public SentinelBalanceTasks() {
        }

        public Map<String, Map<String, SentinelBalanceTask>> getTasks() {
            return tasks;
        }

        public void addTasks(String dc, String tag, SentinelBalanceTask task) {
            if (!tasks.containsKey(dc)) {
                tasks.put(dc, new HashMap<>());
            }
            tasks.get(dc).put(tag, task);
        }

        public SentinelBalanceTask getTaskByDcTag(String dc, String tag) {
            if (tasks.get(dc) == null) {
                return null;
            }
            return tasks.get(dc).get(tag);
        }

        public void removeTask(String dc, String tag) {
            if (tasks.get(dc) != null) {
                tasks.get(dc).remove(tag);
            }
        }
    }

    private static class DcSentinels {

        private long dcId;

        private List<SentinelGroupModel> sentinels;

        public DcSentinels(long dcId) {
            this.dcId = dcId;
            this.sentinels = new ArrayList<>();
        }

        public void addSentinel(SentinelGroupModel sentinelGroup) {
            this.sentinels.add(sentinelGroup);
        }

        public long getDcId() {
            return dcId;
        }

        public List<SentinelGroupModel> getSentinels() {
            return sentinels;
        }
    }

}
