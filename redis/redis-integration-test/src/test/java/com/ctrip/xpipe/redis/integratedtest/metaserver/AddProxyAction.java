package com.ctrip.xpipe.redis.integratedtest.metaserver;

import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.springframework.web.client.RestOperations;

class AddProxyAction {
    String console_url;
    AddProxyAction(String console_url) {
        this.console_url = console_url;
    }
    protected RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(3, 100);
    public boolean addProxy(ProxyModel proxy) {
        restTemplate.put(String.format("http://%s/api/proxy", console_url) , proxy);
        return true;
    }

}