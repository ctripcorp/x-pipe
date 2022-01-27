package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.SentinelGroupInfo;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
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

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author lishanglin
 * date 2021/8/31
 */
@Service
public class DefaultSentinelBalanceService implements SentinelBalanceService {

    private SentinelService sentinelService;

    private DcClusterShardService dcClusterShardService;

    private ScheduledExecutorService balanceScheduled;

    private ConsoleConfig config;

    private ExecutorService executors;

    private TimeBoundCache<Map<String, DcSentinels>> cachedSentinels;

    private Map<String, SentinelBalanceTask> balanceTasks = Maps.newConcurrentMap();

    private static final Logger logger = LoggerFactory.getLogger(DefaultSentinelBalanceService.class);

    @Autowired
    public DefaultSentinelBalanceService(SentinelService sentinelService, DcClusterShardService dcClusterShardService,
                                         ConsoleConfig config, @Qualifier(GLOBAL_EXECUTOR) ExecutorService executors) {
        this.sentinelService = sentinelService;
        this.dcClusterShardService = dcClusterShardService;
        this.config = config;
        this.balanceScheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("SentinelBalanceScheduled"));
        this.executors = executors;
        this.cachedSentinels = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::refreshCache);
    }

    @Override
    public List<SetinelTbl> getCachedDcSentinel(String dcId) {
        Map<String, DcSentinels> sentinels = cachedSentinels.getData(false);
        if (StringUtil.isEmpty(dcId) || !sentinels.containsKey(dcId.toUpperCase())) {
            return Collections.emptyList();
        }

        return sentinels.get(dcId.toUpperCase()).getSentinels();
    }

    @Override
    public SetinelTbl selectSentinel(String dcId) {
        Map<String, DcSentinels> sentinels = cachedSentinels.getData(false);
        if (StringUtil.isEmpty(dcId) || !sentinels.containsKey(dcId.toUpperCase())) {
            return null;
        }

        return selectSentinel(sentinels.get(dcId.toUpperCase()));
    }

    @Override
    public SetinelTbl selectSentinelWithoutCache(String dcId) {
        Map<String, DcSentinels> sentinels = cachedSentinels.getData(true);
        if (StringUtil.isEmpty(dcId) || !sentinels.containsKey(dcId.toUpperCase())) {
            return null;
        }

        return selectSentinel(sentinels.get(dcId.toUpperCase()));
    }

    @Override
    public Map<Long, SetinelTbl> selectMultiDcSentinels() {
        Map<String, DcSentinels> sentinels = cachedSentinels.getData(false);
        Map<Long, SetinelTbl> sentinelMap = new HashMap<>();

        for (DcSentinels dcSentinels: sentinels.values()) {
            DcTbl dcTbl = dcSentinels.getDcTbl();
            sentinelMap.put(dcTbl.getId(), selectSentinel(dcSentinels));
        }
        return sentinelMap;
    }

    private SetinelTbl selectSentinel(DcSentinels dcSentinels) {
        List<SetinelTbl> sentinels = dcSentinels.getSentinels();
        long minCnt = Long.MAX_VALUE;
        SetinelTbl idealSentinel = null;
        for (SetinelTbl sentinel: sentinels) {
            if (sentinel.getShardCount() < minCnt) {
                minCnt = sentinel.getShardCount();
                idealSentinel = sentinel;
            }
        }

        return idealSentinel;
    }

    @Override
    public synchronized void rebalanceDcSentinel(String dc) {
        checkCurrentTask(dc);

        SentinelBalanceTask task = new AllSentinelsBalanceTask(dc, this, dcClusterShardService, balanceScheduled,
                config.getRebalanceSentinelMaxNumOnce(), config.getRebalanceSentinelInterval());
        balanceTasks.put(dc.toUpperCase(), task);
        task.execute(executors);
    }

    @Override
    public synchronized void rebalanceBackupDcSentinel(String dc) {
        checkCurrentTask(dc);

        SentinelBalanceTask task = new BackupDcOnlySentinelBalanceTask(dc, this, dcClusterShardService);
        balanceTasks.put(dc.toUpperCase(), task);
        task.execute(executors);
    }

    @Override
    public synchronized void cancelCurrentBalance(String dc) {
        if (StringUtil.isEmpty(dc)) throw new IllegalArgumentException("unexpected dc " + dc);

        if (balanceTasks.containsKey(dc.toUpperCase())) {
            balanceTasks.get(dc.toUpperCase()).future().cancel(true);
            balanceTasks.remove(dc.toUpperCase());
        }
    }

    private void checkCurrentTask(String dc) {
        if (StringUtil.isEmpty(dc)) throw new IllegalArgumentException("unexpected dc " + dc);

        if (balanceTasks.containsKey(dc.toUpperCase())) {
            SentinelBalanceTask task = balanceTasks.get(dc.toUpperCase());
            if (!task.future().isDone()) {
                throw new IllegalStateException("current task running");
            }
        }
    }

    @Override
    public SentinelBalanceTask getBalanceTask(String dcId) {
        if (StringUtil.isEmpty(dcId) || !balanceTasks.containsKey(dcId.toUpperCase())) {
            return null;
        }

        return balanceTasks.get(dcId.toUpperCase());
    }

    @Override
    public SentinelGroupInfo selectSentinelByDcAndType(String dcId, ClusterType clusterType) {
        return null;
    }

    @Override
    public Map<Long, SentinelGroupInfo> selectMultiDcSentinelsByType(ClusterType clusterType) {
        return null;
    }

    @Override
    public void bindShardAndSentinelsByType(ClusterType clusterType) {
// 把credis中当前哨兵组都同步到xpipe服务，根据从哨兵上获取当前的监控组信息 ，将哨兵组与分片绑定
    }

    private Map<String, DcSentinels> refreshCache() {
        logger.debug("[refreshCache] begin");
        List<SetinelTbl> sentinelTbls = sentinelService.getAllSentinelsWithUsage();
        Map<String, DcSentinels> newCacheData = new HashMap<>();
        for(SetinelTbl sentinelTbl : sentinelTbls) {
            if(StringUtil.isEmpty(sentinelTbl.getSetinelAddress()))
                continue;
            String dcName = sentinelTbl.getDcInfo().getDcName().toUpperCase();
            newCacheData.putIfAbsent(dcName, new DcSentinels(sentinelTbl.getDcInfo()));
            newCacheData.get(dcName).addSentinel(sentinelTbl);
        }

        return newCacheData;
    }

    private static class DcSentinels {

        private DcTbl dcTbl;

        private List<SetinelTbl> sentinels;

        public DcSentinels(DcTbl dcTbl) {
            this.dcTbl = dcTbl;
            this.sentinels = new ArrayList<>();
        }

        public void addSentinel(SetinelTbl setinelTbl) {
            this.sentinels.add(setinelTbl);
        }

        public DcTbl getDcTbl() {
            return dcTbl;
        }

        public List<SetinelTbl> getSentinels() {
            return sentinels;
        }
    }

}
