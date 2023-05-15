package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.model.ProxyPingStatsModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class ProxyChainApiController extends AbstractController {
    String currentDc = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    private ProxyChainAnalyzer analyzer;

    @Autowired
    private ProxyChainCollector collector;

    @Autowired
    private ProxyService proxyService;

    @RequestMapping(value = "/proxy/chains/all", method = RequestMethod.GET)
    public List<ProxyChain> getProxyChains() {
        return collector.getProxyChains();
    }

    @RequestMapping(value = "/proxy/chains/shard", method = RequestMethod.GET)
    public Map<DcClusterShardPeer, ProxyChain> getShardProxyChainMap() {
        return collector.getShardProxyChainMap();
    }

    @RequestMapping(value = "/proxy/chains/dc/shard", method = RequestMethod.GET)
    public Map<String, Map<DcClusterShardPeer, ProxyChain>> getDCShardProxyChainMap() {
        return collector.getDcProxyChainMap();
    }

    @RequestMapping(value = "/proxy/chains/{dcName}", method = RequestMethod.GET)
    public Map<DcClusterShardPeer, ProxyChain> getProxyChainsByDc(@PathVariable String dcName){
        if (!currentDc.equalsIgnoreCase(dcName)) {
            logger.warn("get proxy chain from wrong dc {}", dcName);
            return null;
        }
        return analyzer.getClusterShardChainMap();
    }

    @RequestMapping(value = "/proxy/ping-stats/{dcName}", method = RequestMethod.GET)
    public List<ProxyPingStatsModel> getProxyPingStats(@PathVariable String dcName) {
        return proxyService.getProxyPingStatsModels(dcName);
    }

    @RequestMapping(value = "/proxy/tunnels/{proxyIp}/{dcName}", method = RequestMethod.GET)
    public List<DefaultTunnelInfo> getTunnelModels(@PathVariable String dcName, @PathVariable String proxyIp) {
        return proxyService.getProxyTunnels(dcName, proxyIp);
    }

    @RequestMapping(value = "/proxy/monitor-collectors", method = RequestMethod.GET)
    public List<ProxyMonitorCollector> getAllProxyMonitorCollectors() {
        return proxyService.getAllProxyMonitorCollectors();
    }

}
