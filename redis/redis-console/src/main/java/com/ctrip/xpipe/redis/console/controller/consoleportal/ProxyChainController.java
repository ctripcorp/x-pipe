package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyChainModel;
import com.ctrip.xpipe.redis.console.model.ProxyPingStatsModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyInfoModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.TunnelModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.TunnelSocketStatsMetricOverview;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.console.proxy.TunnelSocketStatsAnalyzerManager;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.support.MetricType;
import org.springframework.web.bind.annotation.*;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class ProxyChainController extends AbstractConsoleController {

    private static final String PROXY_PING_HICKWALL_TEMPLATE = "aliasBy(fx.xpipe.proxy.ping,address)";

    private static final String PROXY_CHAIN_HICKWALL_TEMPLATE = "aliasBy(fx.xpipe.%s;cluster=%s;shard=%s,address)";

    private static final String PROXY_TRAFFIC_HICKWALL_TEMPLATE = "aliasBy(fx.xpipe.proxy.traffic;address='%s:%d',direction)";

    private static final String SUFFIX = "&panel.datasource=incluster&panel.db=FX&panelId=1&fullscreen&edit";

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
                    info.getTunnelStatsResult(), overview));
        }
        return results;
    }

    @RequestMapping(value = "/chain/{backupDcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
    public ProxyChain getProxyChain(@PathVariable String backupDcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return proxyService.getProxyChain(backupDcId, clusterId, shardId);
    }

    @RequestMapping(value = "/chain/{backupDcId}/{clusterId}", method = RequestMethod.GET)
    public List<ProxyChainModel> getProxyChains(@PathVariable String backupDcId, @PathVariable String clusterId) throws ResourceNotFoundException {
        List<ProxyChain> chains = proxyService.getProxyChains(backupDcId, clusterId);
        List<ProxyChainModel> result = Lists.newArrayListWithCapacity(chains.size());
        long activeDcId = clusterService.find(clusterId).getActivedcId();
        String activeDc = dcService.getDcName(activeDcId);
        for(ProxyChain chain : chains) {
            result.add(new ProxyChainModel(chain, activeDc, backupDcId));
        }
        return result;
    }

    @RequestMapping(value = "/proxy/ping/hickwall", method = RequestMethod.GET)
    public Map<String, String> getProxyPingHickwall() {
        String template = null;
        try {
            template = URLEncoder.encode(PROXY_PING_HICKWALL_TEMPLATE, ENDCODE_TYPE);
        } catch (UnsupportedEncodingException e) {
            logger.error("[getHickwallAddress]", e);
            return ImmutableMap.of("addr", "");
        }
        return ImmutableMap.of("addr", getHickwall(template));
    }

    @RequestMapping(value = "/proxy/chain/hickwall/{clusterId}/{shardId}", method = RequestMethod.GET)
    public Map<String, String> getChainHickwall(@PathVariable String clusterId, @PathVariable String shardId) {
        List<String> metricTypes = socketStatsAnalyzerManager.getMetricTypes();
        Map<String, String> result = Maps.newHashMap();
        String template = null;
        for(String metricType : metricTypes) {
            try {
                template = URLEncoder.encode(String.format(PROXY_CHAIN_HICKWALL_TEMPLATE, metricType, clusterId, shardId), ENDCODE_TYPE);
                result.put(metricType, getHickwall(template));
            } catch (UnsupportedEncodingException e) {
                logger.error("[getHickwallAddress]", e);
            }
        }
        return result;
    }

    private String getHickwall(String middle) {
        String prefix = consoleConfig.getHickwallAddress();
        if (Strings.isEmpty(prefix)) {
            return "";
        }
        return prefix + middle + SUFFIX;
    }


    @RequestMapping(value = "/proxy/status/all", method = RequestMethod.GET)
    public List<ProxyInfoModel> getAllProxyInfo() {
        return proxyService.getAllProxyInfo();
    }

    @RequestMapping(value = "/proxy/traffic/hickwall/{host}/{port}", method = RequestMethod.GET)
    public Map<String, String> getProxyTrafficHickwall(@PathVariable String host, @PathVariable int port) {
        String template = null;
        try {
            template = URLEncoder.encode(String.format(PROXY_TRAFFIC_HICKWALL_TEMPLATE, host, port), ENDCODE_TYPE);
        } catch (UnsupportedEncodingException e) {
            logger.error("[getHickwallAddress]", e);
            return ImmutableMap.of("addr", "");
        }
        return ImmutableMap.of("addr", getHickwall(template));
    }

    @RequestMapping(value = "/proxy/chain", method = RequestMethod.DELETE)
    public RetMessage closeProxyChain(@RequestBody List<HostPort> proxies) {
        return proxyService.deleteProxyChain(proxies);
    }

}
