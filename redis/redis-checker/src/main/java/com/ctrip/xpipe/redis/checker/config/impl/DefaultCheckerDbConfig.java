package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.alert.AlertDbConfig;

import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/13
 */
public class DefaultCheckerDbConfig implements CheckerDbConfig, AlertDbConfig {

    private PersistenceCache persistenceCache;

    @Autowired
    public DefaultCheckerDbConfig(PersistenceCache persistenceCache) {
        this.persistenceCache = persistenceCache;
    }

    @Override
    public boolean isAlertSystemOn() {
        return persistenceCache.isAlertSystemOn();
    }

    @Override
    public boolean isSentinelAutoProcess() {
        return persistenceCache.isSentinelAutoProcess();
    }

    @Override
    public boolean shouldSentinelCheck(String cluster) {
        if (StringUtil.isEmpty(cluster)) return false;

        Set<String> whiteList = sentinelCheckWhiteList();
        return null != whiteList && !whiteList.contains(cluster.toLowerCase());
    }

    @Override
    public Set<String> sentinelCheckWhiteList() {
        return this.persistenceCache.sentinelCheckWhiteList();
    }

    @Override
    public boolean shouldClusterAlert(String cluster) {
        if (StringUtil.isEmpty(cluster)) return false;

        Set<String> whiteList = clusterAlertWhiteList();
        return null != whiteList && !whiteList.contains(cluster.toLowerCase());
    }

    @Override
    public Set<String> clusterAlertWhiteList() {
        return this.persistenceCache.clusterAlertWhiteList();
    }
}
