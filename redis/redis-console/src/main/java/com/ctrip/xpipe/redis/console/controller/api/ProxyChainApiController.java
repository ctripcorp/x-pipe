package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class ProxyChainApiController {

    @Autowired
    private ProxyChainAnalyzer analyzer;


    @RequestMapping(value = "/proxy/chains/", method = RequestMethod.GET)
    public List<ProxyChain> getProxyChains() {
        return analyzer.getProxyChains();
    }


}
