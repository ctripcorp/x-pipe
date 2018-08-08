package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NextHopAlgorithm;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class LocalNextHopAlgorithm implements NextHopAlgorithm {

    @Override
    public ProxyEndpoint nextHop(List<ProxyEndpoint> endpoints) {
        for(ProxyEndpoint node : endpoints) {
            if(node.getHost().contains("127.0.0.1") && !node.isSslEnabled()) {
                return node;
            }
        }
        return null;
    }
}
