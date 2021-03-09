package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
@Component
public class DefaultConsoleDbConfig extends AbstractConfigBean implements ConsoleDbConfig{

    private Pair<Set<String>, Long> sentinelCheckWhitelistCache = null;

    private static final long DEFAULT_CACHE_EXPIRED = 10 * 1000L;

    @Autowired
    private DbConfig dbConfig;

    @Autowired
    private ConfigService configService;

    @PostConstruct
    public void postConstruct(){
        setConfig(dbConfig);
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
        return !sentinelCheckWhiteList(disableCache).contains(cluster);
    }

    @Override
    public Set<String> sentinelCheckWhiteList(boolean disableCache) {
        if (!disableCache &&
                null != sentinelCheckWhitelistCache
                && sentinelCheckWhitelistCache.getValue().compareTo(System.currentTimeMillis()) > 0) {
            return sentinelCheckWhitelistCache.getKey();
        }

        List<ConfigModel> configModels = configService.getActiveSentinelCheckExcludeConfig();
        Set<String> whitelist = configModels.stream().map(ConfigModel::getSubKey).collect(Collectors.toSet());
        sentinelCheckWhitelistCache = new Pair<>(whitelist, System.currentTimeMillis() + DEFAULT_CACHE_EXPIRED);
        return whitelist;
    }

}
