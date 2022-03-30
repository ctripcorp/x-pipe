package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.Set;

public abstract class AbstractPersistenceCache implements PersistenceCache {
    protected CheckerConfig config;

    private TimeBoundCache<Set<String>> sentinelCheckWhiteListCache;
    private TimeBoundCache<Set<String>> clusterAlertWhiteListCache;
    private TimeBoundCache<Set<String>> migratingClusterListCache;
    private TimeBoundCache<Boolean> isSentinelAutoProcessCache;
    private TimeBoundCache<Boolean> isAlertSystemOnCache;
    private TimeBoundCache<Map<String, Date>> allClusterCreateTimeCache;

    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    abstract Set<String> doSentinelCheckWhiteList();
    abstract Set<String> doClusterAlertWhiteList();
    abstract Set<String> doGetMigratingClusterList();
    abstract boolean doIsSentinelAutoProcess();
    abstract boolean doIsAlertSystemOn();
    abstract Map<String, Date> doLoadAllClusterCreateTime();
    public AbstractPersistenceCache(CheckerConfig config) {
        initWithConfig(config);
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
    public boolean isClusterOnMigration(String clusterId) {
        return migratingClusterList().contains(clusterId);
    }

    @Override
    public Set<String> migratingClusterList() {
        return migratingClusterListCache.getData(false);
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
    protected void initWithConfig(CheckerConfig config) {
        this.config = config;
        this.sentinelCheckWhiteListCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doSentinelCheckWhiteList);
        this.clusterAlertWhiteListCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doClusterAlertWhiteList);
        this.migratingClusterListCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doGetMigratingClusterList);
        this.isSentinelAutoProcessCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doIsSentinelAutoProcess);
        this.isAlertSystemOnCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doIsAlertSystemOn);
        this.allClusterCreateTimeCache = new TimeBoundCache<>(config::getConfigCacheTimeoutMilli, this::doLoadAllClusterCreateTime);
    }
    
    @VisibleForTesting
    protected CheckerConfig getConfig() {
        return this.config;
    }
    
    
    
}
