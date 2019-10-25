package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.ProxyPingStatsModel;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyInfoModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
public interface ProxyService {

    /**Proxy Database related*/
    List<ProxyModel> getActiveProxies();

    List<ProxyModel> getAllProxies();

    List<ProxyModel> getMonitorActiveProxies();

    void updateProxy(ProxyModel model);

    void deleteProxy(long id);

    void addProxy(ProxyModel model);

    List<ProxyTbl> getActiveProxyTbls();

    /**Proxy Chain related*/
    ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId);

    ProxyChain getProxyChain(String tunnelId);

    List<TunnelInfo> getProxyTunnels(String dcId, String ip);

    List<ProxyChain> getProxyChains(String backupDcId, String clusterId);

    List<ProxyPingStatsModel> getProxyMonitorCollectors(String dcName);

    List<ProxyInfoModel> getAllProxyInfo();

    RetMessage deleteProxyChain(List<HostPort> proxies);
}
