package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.cluster.SentinelType;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBindTask;
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

    private TimeBoundCache<Map<String, SentinelsCache>> cachedSentinels;

    private Map<String, SentinelBalanceTasks> balanceTasks = Maps.newConcurrentMap();

    private Map<String, SentinelBindTask> bindTasks = Maps.newConcurrentMap();

    private static final Logger logger = LoggerFactory.getLogger(DefaultSentinelBalanceService.class);

    @Autowired
    public DefaultSentinelBalanceService(SentinelGroupService sentinelService, DcClusterShardService dcClusterShardService,
                                         ConsoleConfig config, @Qualifier(GLOBAL_EXECUTOR) ExecutorService executors, SentinelManager sentinelManager) {
        this.sentinelService = sentinelService;
        this.dcClusterShardService = dcClusterShardService;
        this.config = config;
        this.balanceScheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("SentinelBalanceScheduled"));
        this.executors = executors;
        this.cachedSentinels = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::refreshCache);
        this.sentinelManager = sentinelManager;
    }

    @Override
    public List<SentinelGroupModel> getCachedDcSentinel(String dcId, SentinelType sentinelType) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);
        if (StringUtil.isEmpty(dcId) || !sentinels.containsKey(sentinelType.name())) {
            return Collections.emptyList();
        }

        SentinelsCache typeSentinelCache = sentinels.get(sentinelType.name());
        DcSentinels dcSentinels = typeSentinelCache.getByDc(dcId.toUpperCase());
        if (dcSentinels == null)
            return Collections.emptyList();

        return dcSentinels.getSentinels();
    }

    @Override
    public SentinelGroupModel selectSentinel(String dcId, SentinelType sentinelType) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);
        if (StringUtil.isEmpty(dcId) || !sentinels.containsKey(sentinelType.name())) {
            return null;
        }

        DcSentinels dcSentinels = sentinels.get(sentinelType.name()).getByDc(dcId);
        if (dcSentinels == null)
            return null;

        return selectSentinel(dcSentinels);
    }

    @Override
    public SentinelGroupModel selectSentinelWithoutCache(String dcId, SentinelType sentinelType) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(true);
        if (StringUtil.isEmpty(dcId) || !sentinels.containsKey(dcId.toUpperCase())) {
            return null;
        }

        DcSentinels dcSentinels = sentinels.get(sentinelType.name()).getByDc(dcId);
        if (dcSentinels == null)
            return null;

        return selectSentinel(dcSentinels);
    }

    @Override
    public Map<Long, SentinelGroupModel> selectMultiDcSentinels(SentinelType sentinelType) {
        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);
        Map<Long, SentinelGroupModel> sentinelMap = new HashMap<>();

        SentinelsCache typeSentinelsCache = sentinels.get(sentinelType.name());
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
    public synchronized void rebalanceDcSentinel(String dc, SentinelType sentinelType) {
        checkCurrentTask(dc, sentinelType);

        SentinelBalanceTask task = new AllSentinelsBalanceTask(dc, this, dcClusterShardService, balanceScheduled,
                config.getRebalanceSentinelMaxNumOnce(), config.getRebalanceSentinelInterval());
        balanceTasks.putIfAbsent(sentinelType.name(), new SentinelBalanceTasks());
        balanceTasks.get(sentinelType.name()).addTasks(dc.toUpperCase(), task);
        task.execute(executors);
    }

    @Override
    public synchronized void rebalanceBackupDcSentinel(String dc) {
        checkCurrentTask(dc, SentinelType.DR_CLUSTER);

        SentinelBalanceTask task = new BackupDcOnlySentinelBalanceTask(dc, this, dcClusterShardService);

        balanceTasks.putIfAbsent(SentinelType.DR_CLUSTER.name(), new SentinelBalanceTasks());
        balanceTasks.get(SentinelType.DR_CLUSTER.name()).addTasks(dc.toUpperCase(), task);

        task.execute(executors);
    }

    @Override
    public synchronized void cancelCurrentBalance(String dc, SentinelType sentinelType) {
        if (StringUtil.isEmpty(dc)) throw new IllegalArgumentException("unexpected dc " + dc);
        if (sentinelType == null) throw new IllegalArgumentException("sentinel type cannot be null");

        if(cachedSentinels.getData(false).get(sentinelType.name())==null) throw new IllegalArgumentException("unexpected sentinel type " + sentinelType.name());

        SentinelBalanceTasks tasks = balanceTasks.get(sentinelType.name());
        if (tasks == null)
            return;
        SentinelBalanceTask task = tasks.getTaskByDc(dc.toUpperCase());
        if (task != null) {
            task.future().cancel(true);
            tasks.removeTask(dc.toUpperCase());
        }
    }

    private void checkCurrentTask(String dc, SentinelType sentinelType) {
        if (StringUtil.isEmpty(dc)) throw new IllegalArgumentException("unexpected dc " + dc);

        if(cachedSentinels.getData(false).get(sentinelType.name())==null) throw new IllegalArgumentException("unexpected sentinel type " + sentinelType.name());

        SentinelBalanceTasks typeTasks = balanceTasks.get(sentinelType.name());
        if (typeTasks == null)
            return;

        SentinelBalanceTask dcTask = typeTasks.getTasks().get(dc.toUpperCase());
        if (dcTask != null) {
            if (!dcTask.future().isDone()) {
                throw new IllegalStateException("current task running");
            }
        }
    }

    private void checkCurrentBindTask(SentinelType sentinelType) {
        if(cachedSentinels.getData(false).get(sentinelType.name())==null) throw new IllegalArgumentException("unexpected sentinel type " + sentinelType.name());

        SentinelBindTask bindTask = bindTasks.get(sentinelType.name());
        if (bindTask != null) {
            if (!bindTask.future().isDone()) {
                throw new IllegalStateException("current task running");
            }
        }
    }

    @Override
    public SentinelBalanceTask getBalanceTask(String dcId, SentinelType sentinelType) {

        SentinelBalanceTasks typeTasks = balanceTasks.get(sentinelType.name());
        if (StringUtil.isEmpty(dcId) || typeTasks == null) {
            return null;
        }

        return typeTasks.getTaskByDc(dcId.toUpperCase());
    }


    @Override
    public void bindShardAndSentinelsByType(SentinelType sentinelType) {
        checkCurrentBindTask(sentinelType);

        Map<String, SentinelsCache> sentinels = cachedSentinels.getData(false);
        SentinelsCache sentinelsCache = sentinels.get(sentinelType.name());

        SentinelBindTask task = new DefaultSentinelBindTask(sentinelManager, dcClusterShardService,
                sentinelsCache.getAllSentinelGroups(), config);

        bindTasks.put(sentinelType.name(), task);
        task.execute(executors);
    }

    private Map<String, SentinelsCache> refreshCache() {
        logger.debug("[refreshCache] begin");
        List<SentinelGroupModel> sentinelGroups = sentinelService.getAllSentinelGroupsWithUsage();

        Map<String, List<SentinelGroupModel>> sentinelsCollectedByType = sentinelGroups.stream().collect(
                Collectors.toMap(SentinelGroupModel::getSentinelType,
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
                    newCacheData.putIfAbsent(dcName, new DcSentinels(dcInfos.get(dcName)));
                    newCacheData.get(dcName).addSentinel(sentinelGroup);
                }
            }
            result.put(k, new SentinelsCache(newCacheData));
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
