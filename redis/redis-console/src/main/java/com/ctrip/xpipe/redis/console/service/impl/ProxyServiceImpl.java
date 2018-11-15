package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.ProxyDao;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.proxy.*;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
@Service
public class ProxyServiceImpl implements ProxyService {

    @Autowired
    private ProxyDao proxyDao;

    @Autowired
    private DcService dcService;

    @Autowired
    private ProxyChainAnalyzer analyzer;

    @Autowired
    private ProxyMonitorCollectorManager proxyMonitorCollectorManager;

    @Autowired
    private ShardService shardService;

    @Override
    public List<ProxyModel> getActiveProxies() {
        List<ProxyTbl> proxyTbls = proxyDao.getActiveProxyTbls();
        List<ProxyModel> proxies = Lists.newArrayListWithCapacity(proxyTbls.size());
        for(ProxyTbl proxy : proxyTbls) {
            proxies.add(ProxyModel.fromProxyTbl(proxy, dcService));
        }
        return proxies;
    }

    @Override
    public List<ProxyModel> getAllProxies() {
        List<ProxyModel> clone = Lists.transform(proxyDao.getAllProxyTbls(), new Function<ProxyTbl, ProxyModel>() {
            @Override
            public ProxyModel apply(ProxyTbl input) {
                return ProxyModel.fromProxyTbl(input, dcService);
            }
        });
        return Lists.newArrayList(clone);
    }

    @Override
    public void updateProxy(ProxyModel model) {
        proxyDao.update(model.toProxyTbl(dcService));
    }

    @Override
    public void deleteProxy(long id) {
        proxyDao.delete(id);
    }

    @Override
    public void addProxy(ProxyModel model) {
        proxyDao.insert(model.toProxyTbl(dcService));
    }

    @Override
    public List<ProxyTbl> getActiveProxyTbls() {
        return proxyDao.getActiveProxyTbls();
    }

    @Override
    public ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId) {
        return analyzer.getProxyChain(backupDcId, clusterId, shardId);
    }

    @Override
    public ProxyChain getProxyChain(String tunnelId) {
        return analyzer.getProxyChain(tunnelId);
    }

    @Override
    public List<TunnelInfo> getProxyTunnels(String dcId, String ip) {
        List<ProxyMonitorCollector> collectors = proxyMonitorCollectorManager.getProxyMonitorResults();
        for(ProxyMonitorCollector collector : collectors) {
            if(collector.getProxyInfo().getDcName().equalsIgnoreCase(dcId)
                    && collector.getProxyInfo().getHostPort().getHost().equalsIgnoreCase(ip)) {
                return collector.getTunnelInfos();
            }
        }
        return Collections.emptyList();
    }

    @Override
    public List<ProxyChain> getProxyChains(String backupDcId, String clusterId) {
        List<ShardTbl> shards = shardService.findAllShardNamesByClusterName(clusterId);
        List<ProxyChain> proxyChains = Lists.newArrayList();
        for(ShardTbl shard : shards) {
            proxyChains.add(analyzer.getProxyChain(backupDcId, clusterId, shard.getShardName()));
        }
        return proxyChains;
    }
}
