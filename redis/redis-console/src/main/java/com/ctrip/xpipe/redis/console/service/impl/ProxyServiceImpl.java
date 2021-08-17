package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.dao.ProxyDao;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyInfoModel;
import com.ctrip.xpipe.redis.console.proxy.*;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyChainAnalyzer;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.utils.ExceptionUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
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

    @Autowired
    private ClusterService clusterService;

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
    public ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId) {
        return analyzer.getProxyChain(backupDcId, clusterId, shardId, peerDcId);
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
    public Map<String, List<ProxyChain>> getProxyChains(String backupDcId, String clusterName) {
        List<ShardTbl> shards = shardService.findAllShardNamesByClusterName(clusterName);
        List<DcTbl> peerDcs = clusterService.getClusterRelatedDcs(clusterName);
        Map<String, List<ProxyChain>> proxyChains = Maps.newHashMap();
        for(ShardTbl shard : shards) {
            List<ProxyChain> peerChains = Lists.newArrayList();
            String shardId = shard.getShardName();
            for (DcTbl peerDcTbl : peerDcs) {
                String peerDc = peerDcTbl.getDcName();
                if (peerDc.equals(backupDcId)) continue;

                ProxyChain chain = analyzer.getProxyChain(backupDcId, clusterName, shardId, peerDc);
                if(chain != null) {
                    peerChains.add(chain);
                }
            }
            proxyChains.put(shardId, peerChains);
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

    @Override
    public List<ProxyTunnelInfo> getAllProxyTunnels() {
        List<ProxyChain> chains = analyzer.getProxyChains();
        List<ProxyTunnelInfo> proxyTunnelInfos = new ArrayList<>();
        chains.forEach(chain -> proxyTunnelInfos.add(chain.buildProxyTunnelInfo()));
        return proxyTunnelInfos;
    }

    @Override
    public ProxyTunnelInfo getProxyTunnelInfo(String backupDcId, String clusterId, String shardId, String peerDcId) {
        ProxyChain chain = analyzer.getProxyChain(backupDcId, clusterId, shardId, peerDcId);
        if (null == chain) return null;
        return chain.buildProxyTunnelInfo();
    }

    @Override
    public void closeProxyTunnel(ProxyTunnelInfo proxyTunnelInfo) {
        deleteProxyChain(proxyTunnelInfo.getBackends());
    }

    private RetMessage notifyProxyNode(HostPort hostPort) {
        String message = null;
        try {
            logger.info("[notifyProxyNode]{} to close port {}", hostPort.getHost(), hostPort.getPort());
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

    @VisibleForTesting
    public ProxyServiceImpl setShardService(ShardService service) {
        this.shardService = service;
        return this;
    }

    @VisibleForTesting
    public ProxyServiceImpl setProxyChainAnalyzer(ProxyChainAnalyzer analyzer) {
        this.analyzer = analyzer;
        return this;
    }
    @VisibleForTesting
    public ProxyServiceImpl setClusterService(ClusterService service) {
        this.clusterService = service;
        return this;
    }
}
