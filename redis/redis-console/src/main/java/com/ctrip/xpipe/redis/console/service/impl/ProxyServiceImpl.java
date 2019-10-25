package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.dao.ProxyDao;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyChainModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyInfoModel;
import com.ctrip.xpipe.redis.console.proxy.*;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.utils.ExceptionUtils;
import com.ctrip.xpipe.utils.StringUtil;
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
public class ProxyServiceImpl extends AbstractService implements ProxyService {

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

    @Override
    public List<ProxyInfoModel> getAllProxyInfo() {
        List<ProxyInfoModel> result = Lists.newArrayList();
        List<ProxyMonitorCollector> proxies = proxyMonitorCollectorManager.getProxyMonitorResults();
        for(ProxyMonitorCollector proxy : proxies) {
            ProxyModel model = proxy.getProxyInfo();
            int chainNum = getChainNumber(proxy);
            result.add(new ProxyInfoModel(model.getHostPort().getHost(), model.getHostPort().getPort(), model.getDcName(), chainNum));
        }
        return result;
    }

    @Override
    public RetMessage deleteProxyChain(List<HostPort> proxies) {
        RetMessage message = null;
        for(HostPort hostPort : proxies) {
            message = notifyProxyNode(hostPort);
            if(message.getState() == RetMessage.SUCCESS_STATE) {
                return message;
            }
        }
        message = message == null ? RetMessage.createFailMessage("no host port received") : message;
        return message;
    }

    private RetMessage notifyProxyNode(HostPort hostPort) {
        String message = null;
        try {
            restTemplate.delete(String.format("http://%s:8080/api/tunnel/local/port/%d", hostPort.getHost(), hostPort.getPort()));
        } catch (Exception e) {
            message = ExceptionUtils.getCause(e).getMessage();
            logger.error("[deleteProxyChain]", e);
            return RetMessage.createFailMessage(message);
        }
        return RetMessage.createSuccessMessage();
    }


    private int getChainNumber(ProxyMonitorCollector proxy) {
        List<TunnelInfo> tunnels = proxy.getTunnelInfos();
        int result = 0;
        for(TunnelInfo info : tunnels) {
            if(analyzer.getProxyChain(info.getTunnelId()) != null) {
                result ++;
            }
        }
        return result;
    }
}
