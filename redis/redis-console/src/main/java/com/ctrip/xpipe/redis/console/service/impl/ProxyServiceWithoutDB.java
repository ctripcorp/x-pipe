package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyInfoModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class ProxyServiceWithoutDB extends ProxyServiceImpl implements ProxyService {

    private ConsolePortalService consolePortalService;

    private ConsoleConfig config;

    private FoundationService foundationService;

    public ProxyServiceWithoutDB(ConsolePortalService consolePortalService, ConsoleConfig config, FoundationService foundationService) {
        this.consolePortalService = consolePortalService;
        this.config = config;
        this.foundationService = foundationService;
    }

    private TimeBoundCache<List<ProxyModel>> allProxyCache;

    private TimeBoundCache<List<ProxyTunnelInfo>> allTunnelCache;

    private TimeBoundCache<List<ProxyModel>> currentDcActiveProxy;

    @PostConstruct
    public void postConstruct(){
        allProxyCache = new TimeBoundCache<>(config::getCacheRefreshInterval, consolePortalService::getAllProxy);

        currentDcActiveProxy = new TimeBoundCache<>(config::getCacheRefreshInterval, consolePortalService::getMonitorActiveProxiesByDc);

        allTunnelCache = new TimeBoundCache<>(config::getCacheRefreshInterval, consolePortalService::findAllTunnelInfo);
    }

    @Override
    public List<ProxyModel> getActiveProxies() {
        return allProxyCache.getData().
                stream().
                filter(model -> model.isActive()).
                collect(Collectors.toList());
    }

    @Override
    public List<ProxyModel> getAllProxies() {
        return allProxyCache.getData();
    }

    @Override
    public List<ProxyModel> getMonitorActiveProxiesByDc(String dcName) {
        if(StringUtil.trimEquals(dcName, foundationService.getDataCenter(), true)) {
            return currentDcActiveProxy.getData();
        } else {
            return allProxyCache.getData().
                    stream().
                    filter(model -> model.isMonitorActive()
                            && StringUtil.trimEquals(model.getDcName(), dcName)).
                    collect(Collectors.toList());
        }
    }

    @Override
    public void updateProxy(ProxyModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteProxy(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addProxy(ProxyModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ProxyTbl> getActiveProxyTbls() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getActiveProxyUrisByDc(String dcName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProxyChain getProxyChain(String tunnelId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<ProxyChain>> getProxyChains(String backupDcId, String clusterId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ProxyMonitorCollector> getAllProxyMonitorCollectors() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ProxyInfoModel> getAllProxyInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RetMessage deleteProxyChain(List<HostPort> proxies) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, String> proxyIdUriMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> proxyUriIdMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ProxyTunnelInfo> getAllProxyTunnels() {
        return allTunnelCache.getData();
    }

    @Override
    public ProxyTunnelInfo getProxyTunnelInfo(String backupDcId, String clusterId, String shardId, String peerDcId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeProxyTunnel(ProxyTunnelInfo proxyTunnelInfo) {
        throw new UnsupportedOperationException();
    }
}
