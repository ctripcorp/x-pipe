package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBindTask;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.SentinelGroupService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
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

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author lishanglin
 * date 2021/8/31
 */
@Service
public class DefaultSentinelBalanceService implements SentinelBalanceService {

    private SentinelGroupService sentinelService;

    private SentinelManager sentinelManager;

    private DcClusterShardService dcClusterShardService;

    private ScheduledExecutorService balanceScheduled;

    private ConsoleConfig config;

    private ExecutorService executors;

    private MetaCache metaCache;

    private TimeBoundCache<Map<String, SentinelsCache>> cachedSentinels;

    private Map<String, SentinelBalanceTasks> balanceTasks = Maps.newConcurrentMap();

    private Map<String, SentinelBindTask> bindTasks = Maps.newConcurrentMap();

    private static final Logger logger = LoggerFactory.getLogger(DefaultSentinelBalanceService.class);

    @Autowired
    public DefaultSentinelBalanceService(SentinelGroupService sentinelService, DcClusterShardService dcClusterShardService,
                                         ConsoleConfig config, @Qualifier(GLOBAL_EXECUTOR) ExecutorService executors, SentinelManager sentinelManager, MetaCache metaCache) {
        this.sentinelService = sentinelService;
        this.dcClusterShardService = dcClusterShardService;
        this.config = config;
        this.balanceScheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("SentinelBalanceScheduled"));
        this.executors = executors;
        this.cachedSentinels = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::refreshCache);
        this.sentinelManager = sentinelManager;
        this.metaCache = metaCache;
    }

    @Override
    public List<SentinelGroupModel> getCachedDcSentinel(String dcId, ClusterType clusterType) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);
        if (StringUtil.isEmpty(dcId) || !sentinels.containsKey(clusterType.name().toUpperCase())) {
            return Collections.emptyList();
        }

        SentinelsCache typeSentinelCache = sentinels.get(clusterType.name().toUpperCase());
        DcSentinels dcSentinels = typeSentinelCache.getByDc(dcId.toUpperCase());
        if (dcSentinels == null)
            return Collections.emptyList();

        return dcSentinels.getSentinels();
    }

    @Override
    public SentinelGroupModel selectSentinel(String dcId, ClusterType clusterType) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);
        if (StringUtil.isEmpty(dcId) || !sentinels.containsKey(clusterType.name().toUpperCase())) {
            return null;
        }

        DcSentinels dcSentinels = sentinels.get(clusterType.name().toUpperCase()).getByDc(dcId.toUpperCase());
        if (dcSentinels == null)
            return null;

        return selectSentinel(dcSentinels);
    }

    @Override
    public SentinelGroupModel selectSentinelWithoutCache(String dcId, ClusterType clusterType) {
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

        return selectSentinel(dcSentinels);
    }

    @Override
    public Map<Long, SentinelGroupModel> selectMultiDcSentinels(ClusterType clusterType) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);
        Map<Long, SentinelGroupModel> sentinelMap = new HashMap<>();

        SentinelsCache typeSentinelsCache = sentinels.get(clusterType.name().toUpperCase());
        if (typeSentinelsCache == null) {
            return sentinelMap;
        }

        for (DcSentinels dcSentinels: typeSentinelsCache.getDcSentinelsMap().values()) {
            sentinelMap.put(dcSentinels.getDcId(), selectSentinel(dcSentinels));
        }
        return sentinelMap;
    }

    private SentinelGroupModel selectSentinel(DcSentinels dcSentinels) {
        if (dcSentinels == null)
            return null;
        List<SentinelGroupModel> sentinels = dcSentinels.getSentinels();
        long minCnt = Long.MAX_VALUE;
        SentinelGroupModel idealSentinel = null;
        for (SentinelGroupModel sentinel: sentinels) {
            if (sentinel.getShardCount() < minCnt) {
                minCnt = sentinel.getShardCount();
                idealSentinel = sentinel;
            }
        }

        return idealSentinel;
    }

    @Override
    public synchronized void rebalanceDcSentinel(String dc, ClusterType clusterType) {
        checkCurrentTask(dc, clusterType);

        SentinelBalanceTask task = new AllSentinelsBalanceTask(dc, this, dcClusterShardService, balanceScheduled,
                config.getRebalanceSentinelMaxNumOnce(), config.getRebalanceSentinelInterval());
        balanceTasks.putIfAbsent(clusterType.name().toUpperCase(), new SentinelBalanceTasks());
        balanceTasks.get(clusterType.name().toUpperCase()).addTasks(dc.toUpperCase(), task);
        task.execute(executors);
    }

    @Override
    public synchronized void rebalanceBackupDcSentinel(String dc) {
        checkCurrentTask(dc, ClusterType.ONE_WAY);

        SentinelBalanceTask task = new BackupDcOnlySentinelBalanceTask(dc, this, dcClusterShardService);

        balanceTasks.putIfAbsent(ClusterType.ONE_WAY.name().toUpperCase(), new SentinelBalanceTasks());
        balanceTasks.get(ClusterType.ONE_WAY.name().toUpperCase()).addTasks(dc.toUpperCase(), task);

        task.execute(executors);
    }

    @Override
    public synchronized void cancelCurrentBalance(String dc, ClusterType clusterType) {
        if (StringUtil.isEmpty(dc)) throw new IllegalArgumentException("unexpected dc " + dc);
        if (clusterType == null) throw new IllegalArgumentException("sentinel type cannot be null");

        if(cachedSentinels.getData(false).get(clusterType.name().toUpperCase())==null) throw new IllegalArgumentException("unexpected cluster type " + clusterType.name());

        SentinelBalanceTasks tasks = balanceTasks.get(clusterType.name().toUpperCase());
        if (tasks == null)
            return;
        SentinelBalanceTask task = tasks.getTaskByDc(dc.toUpperCase());
        if (task != null) {
            task.future().cancel(true);
            tasks.removeTask(dc.toUpperCase());
        }
    }

    private void checkCurrentTask(String dc, ClusterType clusterType) {
        if (StringUtil.isEmpty(dc)) throw new IllegalArgumentException("unexpected dc " + dc);

        if(cachedSentinels.getData(false).get(clusterType.name().toUpperCase())==null) throw new IllegalArgumentException("unexpected cluster type " + clusterType.name());

        SentinelBalanceTasks typeTasks = balanceTasks.get(clusterType.name().toUpperCase());
        if (typeTasks == null)
            return;

        SentinelBalanceTask dcTask = typeTasks.getTasks().get(dc.toUpperCase());
        if (dcTask != null) {
            if (!dcTask.future().isDone()) {
                throw new IllegalStateException("current task running");
            }
        }
    }

    private void checkCurrentBindTask(ClusterType clusterType) {
        if(cachedSentinels.getData(false).get(clusterType.name().toUpperCase())==null) throw new IllegalArgumentException("unexpected cluster type " + clusterType.name());

        SentinelBindTask bindTask = bindTasks.get(clusterType.name().toUpperCase());
        if (bindTask != null) {
            if (!bindTask.future().isDone()) {
                throw new IllegalStateException("current task running");
            }
        }
    }

    @Override
    public SentinelBalanceTask getBalanceTask(String dcId, ClusterType clusterType) {

        SentinelBalanceTasks typeTasks = balanceTasks.get(clusterType.name().toUpperCase());
        if (StringUtil.isEmpty(dcId) || typeTasks == null) {
            return null;
        }

        return typeTasks.getTaskByDc(dcId.toUpperCase());
    }


    @Override
    public void bindShardAndSentinelsByType(ClusterType clusterType) {
        checkCurrentBindTask(clusterType);

        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);
        SentinelsCache sentinelsCache = sentinels.get(clusterType.name().toUpperCase());

        SentinelBindTask task = new DefaultSentinelBindTask(sentinelManager, dcClusterShardService,
                sentinelsCache.getAllSentinelGroups(), config, metaCache);

        bindTasks.put(clusterType.name().toUpperCase(), task);
        task.execute(executors);
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
        private Map<String, SentinelBalanceTask> tasks = new HashMap<>();

        public SentinelBalanceTasks() {
        }

        public Map<String, SentinelBalanceTask> getTasks() {
            return tasks;
        }

        public void addTasks(String dc, SentinelBalanceTask task) {
            this.tasks.put(dc, task);
        }

        public SentinelBalanceTask getTaskByDc(String dc) {
            return tasks.get(dc);
        }

        public void removeTask(String dc) {
            tasks.remove(dc);
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
