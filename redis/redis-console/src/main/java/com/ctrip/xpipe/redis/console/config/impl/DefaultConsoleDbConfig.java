package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.redis.checker.alert.AlertDbConfig;
import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
public class DefaultConsoleDbConfig extends AbstractConfigBean implements ConsoleDbConfig, AlertDbConfig {

    private TimeBoundCache<Set<String>> sentinelCheckWhitelistCache;

    private TimeBoundCache<Set<String>> clusterAlertWhitelistCache;

    @Autowired
    private DbConfig dbConfig;

    @Autowired
    private ConsoleConfig config;

    @Autowired
    private ConfigService configService;

    @PostConstruct
    public void postConstruct(){
        setConfig(dbConfig);

        sentinelCheckWhitelistCache = new TimeBoundCache<>(config::getCacheRefreshInterval, this::refreshSentinelCheckWhiteList);
        clusterAlertWhitelistCache = new TimeBoundCache<>(config::getCacheRefreshInterval, this::refreshClusterAlertWhiteList);
    }


    @Override
    public boolean isSentinelAutoProcess() {
        return configService.isSentinelAutoProcess();
    }

    @Override
    public boolean isAlertSystemOn() {
        return configService.isAlertSystemOn();
    }

    @Override
    public boolean shouldSentinelCheck(String cluster) {
        return shouldSentinelCheck(cluster, false);
    }

    @Override
    public Set<String> sentinelCheckWhiteList() {
        return sentinelCheckWhiteList(false);
    }

    @Override
    public boolean shouldSentinelCheck(String cluster, boolean disableCache) {
        return !sentinelCheckWhiteList(disableCache).contains(cluster.toLowerCase());
    }

    @Override
    public Set<String> sentinelCheckWhiteList(boolean disableCache) {
        return sentinelCheckWhitelistCache.getData(disableCache);
    }

    @Override
    public boolean shouldClusterAlert(String cluster) {
        return !clusterAlertWhiteList().contains(cluster.toLowerCase());
    }

    @Override
    public Set<String> clusterAlertWhiteList() {
        return clusterAlertWhitelistCache.getData(false);
    }

    public void refreshAlertWhiteListCache() {
        clusterAlertWhitelistCache.getData(true);
    }

    private Set<String> refreshSentinelCheckWhiteList() {
        List<ConfigModel> configModels = configService.getActiveSentinelCheckExcludeConfig();
        return configModels.stream().map(model -> model.getSubKey().toLowerCase()).collect(Collectors.toSet());
    }

    private Set<String> refreshClusterAlertWhiteList() {
        List<ConfigModel> configModels = configService.getActiveClusterAlertExcludeConfig();
        return configModels.stream().map(model -> model.getSubKey().toLowerCase()).collect(Collectors.toSet());
    }

}
