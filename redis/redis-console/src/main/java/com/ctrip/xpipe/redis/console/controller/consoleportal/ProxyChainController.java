package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ProxyPingStatsModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyChainModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyInfoModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.TunnelModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.TunnelSocketStatsMetricOverview;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.console.proxy.TunnelSocketStatsAnalyzerManager;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class ProxyChainController extends AbstractConsoleController {

    private static final String PROXY_PING_HICKWALL_TEMPLATE = "&panelId=%d";

    private static final String PROXY_CHAIN_HICKWALL_TEMPLATE = "&panelId=%d&var-measure=%s&var-cluster=%s&var-shard=%s&var-dstDc=%s";

    private static final String PROXY_TRAFFIC_HICKWALL_TEMPLATE = "&panelId=%d&var-address=%s:%d";

    private static final String ENDCODE_TYPE = "UTF-8";

    @Autowired
    private ProxyService proxyService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private TunnelSocketStatsAnalyzerManager socketStatsAnalyzerManager;

    @Autowired
    private DcService dcService;

    @Autowired
    private ConsoleConfig consoleConfig;


    @RequestMapping(value = "/proxy/collectors/{dcName}", method = RequestMethod.GET)
    public List<ProxyPingStatsModel> getProxyMonitorCollectors(@PathVariable String dcName) {
        return proxyService.getProxyMonitorCollectors(dcName);
    }

    @RequestMapping(value = "/proxy/{proxyIp}/{dcName}", method = RequestMethod.GET)
    public List<TunnelModel> getTunnelModels(@PathVariable String dcName, @PathVariable String proxyIp) {
        logger.info("[getTunnelModels] {}, {}", dcName, proxyIp);
        List<TunnelInfo> tunnelInfos = proxyService.getProxyTunnels(dcName, proxyIp);
        if(tunnelInfos == null) {
            return Collections.emptyList();
        }
        List<TunnelModel> results = Lists.newArrayListWithCapacity(tunnelInfos.size());
        for(TunnelInfo info : tunnelInfos) {
            ProxyChain chain = proxyService.getProxyChain(info.getTunnelId());
            if(chain == null) {
                logger.warn("[tunnelId] {}, no chains", info.getTunnelId());
                continue;
            }
            TunnelSocketStatsMetricOverview overview = socketStatsAnalyzerManager.analyze(info.getTunnelSocketStatsResult());
            results.add(new TunnelModel(info.getTunnelId(), chain.getBackupDc(), chain.getCluster(), chain.getShard(),
                    chain.getPeerDcId(), info.getTunnelStatsResult(), overview));
        }
        return results;
    }

    @RequestMapping(value = "/chain/{backupDcId}/{clusterId}/{shardId}/{peerDcId}", method = RequestMethod.GET)
    public ProxyChain getProxyChain(@PathVariable String backupDcId, @PathVariable String clusterId, @PathVariable String shardId, @PathVariable String peerDcId) {
        return proxyService.getProxyChain(backupDcId, clusterId, shardId, peerDcId);
    }

    @RequestMapping(value = "/chain/{backupDcId}/{clusterId}", method = RequestMethod.GET)
    public Map<String,List<ProxyChainModel>> getProxyChains(@PathVariable String backupDcId, @PathVariable String clusterId) throws ResourceNotFoundException {
        Map<String, List<ProxyChain>> chains = proxyService.getProxyChains(backupDcId, clusterId);
        Map<String, List<ProxyChainModel>> result = Maps.newHashMap();

        for(Map.Entry<String, List<ProxyChain>> shardChains : chains.entrySet()) {
            List<ProxyChain> peerChains = shardChains.getValue();
            List<ProxyChainModel> peerResult = Lists.newArrayListWithCapacity(peerChains.size());
            for (ProxyChain chain: peerChains) {
                peerResult.add(new ProxyChainModel(chain, chain.getPeerDcId() , backupDcId));
            }
            result.put(shardChains.getKey(), peerResult);
        }
        return result;
    }

    @RequestMapping(value = "/proxy/ping/hickwall", method = RequestMethod.GET)
    public Map<String, String> getProxyPingHickwall() {
        String template = null;
        try {
            template = String.format(PROXY_PING_HICKWALL_TEMPLATE, consoleConfig.getHickwallMetricInfo().getProxyPingPanelId());
        } catch (Exception e) {
            logger.error("[getHickwallAddress]", e);
            return ImmutableMap.of("addr", "");
        }
        return ImmutableMap.of("addr", getHickwall(template));
    }

    @RequestMapping(value = {"/proxy/chain/hickwall/{clusterId}/{shardId}/{dstDc}",
                            "/proxy/chain/hickwall/{clusterId}/{shardId}"}, method = RequestMethod.GET)
    public Map<String, String> getChainHickwall(@PathVariable String clusterId, @PathVariable String shardId,
                                                @PathVariable(required = false) String dstDc) {
        List<String> metricTypes = socketStatsAnalyzerManager.getMetricTypes();
        Map<String, String> result = Maps.newHashMap();
        String template = null;
        for(String metricType : metricTypes) {
            try {
                template = String.format(PROXY_CHAIN_HICKWALL_TEMPLATE,
                        consoleConfig.getHickwallMetricInfo().getProxyCollectionPanelId(),
                        metricType + "_value", clusterId, shardId, dstDc);
                result.put(metricType, getHickwall(template));
            } catch (Exception e) {
                logger.error("[getHickwallAddress]", e);
            }
        }
        return result;
    }

    private String getHickwall(String middle) {
        String prefix = consoleConfig.getHickwallMetricInfo().getDomain();
        if (Strings.isEmpty(prefix)) {
            return "";
        }
        return prefix + middle;
    }


    @RequestMapping(value = "/proxy/status/all", method = RequestMethod.GET)
    public List<ProxyInfoModel> getAllProxyInfo() {
        return proxyService.getAllProxyInfo();
    }

    @RequestMapping(value = "/proxy/traffic/hickwall/{host}/{port}", method = RequestMethod.GET)
    public Map<String, String> getProxyTrafficHickwall(@PathVariable String host, @PathVariable int port) {
        String template = null;
        try {
            template = String.format(PROXY_TRAFFIC_HICKWALL_TEMPLATE, consoleConfig.getHickwallMetricInfo().getProxyTrafficPanelId(), host, port);
        } catch (Exception e) {
            logger.error("[getHickwallAddress]", e);
            return ImmutableMap.of("addr", "");
        }
        return ImmutableMap.of("addr", getHickwall(template));
    }

    @RequestMapping(value = "/proxy/chain", method = RequestMethod.DELETE)
    public RetMessage closeProxyChain(@RequestBody List<HostPort> proxies) {
        return proxyService.deleteProxyChain(proxies);
    }

    @RequestMapping(value = "/active/proxy/uri/{dcName}", method = RequestMethod.GET)
    public List<String> getActiveProxyUrisByDc(@PathVariable String dcName) {
        logger.info("[getActiveProxyUrisByDc]{}", dcName);
        try {
            return proxyService.getActiveProxyUrisByDc(dcName);
        } catch (Throwable th) {
            logger.error("[getActiveProxyUrisByDc]:{}", dcName, th);
            return Collections.emptyList();
        }
    }

}
