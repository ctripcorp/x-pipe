package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainCollector;
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

    @RequestMapping(value = "/proxy/chains/", method = RequestMethod.GET)
    public List<ProxyChain> getProxyChains() {
        return collector.getProxyChains();
    }

    @RequestMapping(value = "/proxy/chains/{dcName}", method = RequestMethod.GET)
    public Map<DcClusterShardPeer, ProxyChain> getProxyChainsByDc(@PathVariable String dcName){
        if (!currentDc.equalsIgnoreCase(dcName)) {
            logger.warn("get proxy chain from wrong dc {}", dcName);
            return null;
        }
        return analyzer.getClusterShardChainMap();
    }
}
