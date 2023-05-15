package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.ProxyManager;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.ProxyPingStatsModel;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyInfoModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo;

import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
public interface ProxyService extends ProxyManager {

    /**Proxy Database related*/
    List<ProxyModel> getActiveProxies();

    List<ProxyModel> getAllProxies();

    List<ProxyModel> getMonitorActiveProxiesByDc(String dcName);

    void updateProxy(ProxyModel model);

    void deleteProxy(long id);

    void addProxy(ProxyModel model);

    List<ProxyTbl> getActiveProxyTbls();

    List<String> getActiveProxyUrisByDc(String dcName);

    /**Proxy Chain related*/
    ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId);

    ProxyChain getProxyChain(String tunnelId);

    List<DefaultTunnelInfo> getProxyTunnels(String dcId, String ip);

    Map<String, List<ProxyChain>> getProxyChains(String backupDcId, String clusterId);

    List<ProxyPingStatsModel> getProxyPingStatsModels(String dcName);

    List<ProxyMonitorCollector> getAllProxyMonitorCollectors();

    List<ProxyInfoModel> getAllProxyInfo();

    RetMessage deleteProxyChain(List<HostPort> proxies);

    Map<Long, String> proxyIdUriMap();

    Map<String, Long> proxyUriIdMap();
}
