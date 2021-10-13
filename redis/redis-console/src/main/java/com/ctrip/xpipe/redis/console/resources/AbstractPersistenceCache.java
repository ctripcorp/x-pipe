package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

public abstract class AbstractPersistenceCache implements PersistenceCache {
    protected CheckerConfig config;

    DynamicDelayPeriodTask loadCacheTask;
    TimeBoundCache<Set<String>> sentinelCheckWhiteListCache;
    TimeBoundCache<Set<String>> clusterAlertWhiteListCache;
    TimeBoundCache<Boolean> isSentinelAutoProcessCache;
    TimeBoundCache<Boolean> isAlertSystemOnCache;
    TimeBoundCache<Map<String, Date>> allClusterCreateTimeCache;
    
    abstract Set<String> doSentinelCheckWhiteList();
    abstract Set<String> doClusterAlertWhiteList();
    abstract boolean doIsSentinelAutoProcess();
    abstract boolean doIsAlertSystemOn();
    abstract Map<String, Date> doLoadAllClusterCreateTime();
    public AbstractPersistenceCache(CheckerConfig config, ScheduledExecutorService scheduled) {
        setConfig(config);
        this.loadCacheTask = new DynamicDelayPeriodTask("persistenceCacheLoader", this::loadCache, config::getCheckerMetaRefreshIntervalMilli, scheduled);
    }

    private void loadCache() {
        //update
        sentinelCheckWhiteListCache.getData(true);
        clusterAlertWhiteListCache.getData(true);
        isSentinelAutoProcessCache.getData(true);
        isAlertSystemOnCache.getData(true);
        allClusterCreateTimeCache.getData(true);
    }

    @Override
    public Set<String> sentinelCheckWhiteList() {
        return sentinelCheckWhiteListCache.getData(false);
    }

    @Override
    public Set<String> clusterAlertWhiteList() {
        return clusterAlertWhiteListCache.getData(false);
    }

    @Override
    public boolean isSentinelAutoProcess() {
        return isSentinelAutoProcessCache.getData(false);
    }

    @Override
    public boolean isAlertSystemOn() {
        return isAlertSystemOnCache.getData(false);
    }

    @Override
    public Date getClusterCreateTime(String clusterId) {
        Map<String, Date> dates = allClusterCreateTimeCache.getData(false);
        return dates.get(clusterId);
    }

    @Override
    public Map<String, Date> loadAllClusterCreateTime() {
        return allClusterCreateTimeCache.getData(false);
    }

    @VisibleForTesting
    protected void setConfig(CheckerConfig config) {
        this.config = config;
        this.sentinelCheckWhiteListCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doSentinelCheckWhiteList);
        this.clusterAlertWhiteListCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doClusterAlertWhiteList);
        this.isSentinelAutoProcessCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doIsSentinelAutoProcess);
        this.isAlertSystemOnCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doIsAlertSystemOn);
        this.allClusterCreateTimeCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doLoadAllClusterCreateTime);
    }
    
    @VisibleForTesting
    protected CheckerConfig getConfig() {
        return this.config;
    }
    
    
    
}
