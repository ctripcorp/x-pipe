package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.redis.core.proxy.endpoint.NextJumpAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class LocalNextJumpAlgorithm implements NextJumpAlgorithm {

    @Override
    public ProxyEndpoint nextJump(List<ProxyEndpoint> endpoints) {
        for(ProxyEndpoint node : endpoints) {
            if(node.getHost().contains("127.0.0.1") && !node.isSslEnabled()) {
                return node;
            }
        }
        return null;
    }
}
