package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.checker.alert.AlertDbConfig;
import com.ctrip.xpipe.redis.checker.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2021/3/13
 */
public class DefaultCheckerDbConfig implements CheckerDbConfig, AlertDbConfig {

    private Persistence persistence;

    private TimeBoundCache<Set<String>> sentinelCheckWhiteListCache;

    private TimeBoundCache<Boolean> sentinelAutoProcessCache;

    private TimeBoundCache<Boolean> alertSystemOn;

    private TimeBoundCache<Set<String>> clusterAlertWhiteListCache;

    public DefaultCheckerDbConfig(Persistence persistence, LongSupplier timeoutMilliSupplier) {
        this.persistence = persistence;

        alertSystemOn = new TimeBoundCache<>(timeoutMilliSupplier, this.persistence::isAlertSystemOn);
        sentinelAutoProcessCache = new TimeBoundCache<>(timeoutMilliSupplier, this.persistence::isSentinelAutoProcess);
        sentinelCheckWhiteListCache = new TimeBoundCache<>(timeoutMilliSupplier,
                () -> this.lowCaseClusters(persistence.sentinelCheckWhiteList()));
        clusterAlertWhiteListCache = new TimeBoundCache<>(timeoutMilliSupplier,
                () -> this.lowCaseClusters(persistence.clusterAlertWhiteList()));
    }

    private Set<String> lowCaseClusters(Set<String> clusters) {
        return clusters.stream().map(String::toLowerCase).collect(Collectors.toSet());
    }

    @Autowired
    public DefaultCheckerDbConfig(Persistence persistence, CheckerConfig config) {
        this(persistence, config::getConfigCacheTimeoutMilli);
    }

    @Override
    public boolean isAlertSystemOn() {
        return alertSystemOn.getData(false);
    }

    @Override
    public boolean isSentinelAutoProcess() {
        return sentinelAutoProcessCache.getData(false);
    }

    @Override
    public boolean shouldSentinelCheck(String cluster) {
        if (StringUtil.isEmpty(cluster)) return false;

        Set<String> whiteList = sentinelCheckWhiteList();
        return null != whiteList && !whiteList.contains(cluster.toLowerCase());
    }

    @Override
    public Set<String> sentinelCheckWhiteList() {
        return sentinelCheckWhiteListCache.getData(false);
    }

    @Override
    public boolean shouldClusterAlert(String cluster) {
        if (StringUtil.isEmpty(cluster)) return false;

        Set<String> whiteList = clusterAlertWhiteList();
        return null != whiteList && !whiteList.contains(cluster.toLowerCase());
    }

    @Override
    public Set<String> clusterAlertWhiteList() {
        return clusterAlertWhiteListCache.getData(false);
    }
}
