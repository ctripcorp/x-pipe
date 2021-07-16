package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.ProxyManager;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.ProxyPingStatsModel;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyInfoModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;

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

    List<ProxyModel> getMonitorActiveProxies();

    void updateProxy(ProxyModel model);

    void deleteProxy(long id);

    void addProxy(ProxyModel model);

    List<ProxyTbl> getActiveProxyTbls();

    /**Proxy Chain related*/
    ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId);

    ProxyChain getProxyChain(String tunnelId);

    List<TunnelInfo> getProxyTunnels(String dcId, String ip);

    Map<String, List<ProxyChain>> getProxyChains(String backupDcId, String clusterId);

    List<ProxyPingStatsModel> getProxyMonitorCollectors(String dcName);

    List<ProxyInfoModel> getAllProxyInfo();

    RetMessage deleteProxyChain(List<HostPort> proxies);
}
