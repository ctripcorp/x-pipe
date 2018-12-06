package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.ProxyDao;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.proxy.*;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
        return convert(proxyDao.getActiveProxyTbls());
    }

    @Override
    public List<ProxyModel> getAllProxies() {
        return convert(proxyDao.getAllProxyTbls());
    }

    @Override
    public List<ProxyModel> getMonitorActiveProxies() {
        return convert(proxyDao.getMonitorActiveProxyTbls());
    }

    private List<ProxyModel> convert(List<ProxyTbl> proxyTbls) {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        List<ProxyModel> clone = proxyTbls.stream().map(input -> {
            assert input != null;
            return ProxyModel.fromProxyTbl(input, mapper);
        }).collect(Collectors.toList());
        return Lists.newArrayList(clone);
    }

    @Override
    public void updateProxy(ProxyModel model) {
        DcIdNameMapper mapper = new DcIdNameMapper.OneTimeMapper(dcService);
        proxyDao.update(model.toProxyTbl(mapper));
    }

    @Override
    public void deleteProxy(long id) {
        proxyDao.delete(id);
    }

    @Override
    public void addProxy(ProxyModel model) {
        DcIdNameMapper mapper = new DcIdNameMapper.OneTimeMapper(dcService);
        proxyDao.insert(model.toProxyTbl(mapper));
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
            ProxyChain chain = analyzer.getProxyChain(backupDcId, clusterId, shard.getShardName());
            if(chain != null) {
                proxyChains.add(chain);
            }
        }
        return proxyChains;
    }

    @Override
    public List<ProxyPingStatsModel> getProxyMonitorCollectors(String dcName) {
        if(StringUtil.isEmpty(dcName)) {
            return Collections.emptyList();
        }
        List<ProxyMonitorCollector> collectors = proxyMonitorCollectorManager.getProxyMonitorResults();
        List<ProxyPingStatsModel> result = Lists.newArrayListWithCapacity(collectors.size());
        for(ProxyMonitorCollector collector : collectors) {
            if(dcName.equalsIgnoreCase(collector.getProxyInfo().getDcName())) {
                result.add(new ProxyPingStatsModel(collector.getProxyInfo(), collector.getPingStatsResults()));
            }
        }
        return result;
    }
}
