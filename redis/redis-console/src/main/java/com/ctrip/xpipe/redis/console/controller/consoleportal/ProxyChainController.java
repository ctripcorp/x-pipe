package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.TunnelModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class ProxyChainController extends AbstractConsoleController {

    @Autowired
    private ProxyService proxyService;

    @RequestMapping(value = "/{dcName}/{proxyIp}", method = RequestMethod.GET)
    public List<TunnelModel> getTunnelModels(@PathVariable String dcName, @PathVariable String proxyIp) {
        List<TunnelInfo> tunnelInfos = proxyService.getProxyTunnels(dcName, proxyIp);
        List<TunnelModel> results = Lists.newArrayListWithCapacity(tunnelInfos.size());
        for(TunnelInfo info : tunnelInfos) {
            ProxyChain chain = proxyService.getProxyChain(info.getTunnelId());
            results.add(new TunnelModel(info.getTunnelId(), chain.getBackupDc(), chain.getCluster(), chain.getShard(), info));
        }
        return results;
    }

    @RequestMapping(value = "/chain/{backupDcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
    public ProxyChain getProxyChain(@PathVariable String backupDcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return proxyService.getProxyChain(backupDcId, clusterId, shardId);
    }

    @RequestMapping(value = "/chain/{backupDcId}/{clusterId}", method = RequestMethod.GET)
    public List<ProxyChain> getProxyChains(@PathVariable String backupDcId, @PathVariable String clusterId) {
        return proxyService.getProxyChains(backupDcId, clusterId);
    }
}
