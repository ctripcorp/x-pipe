package com.ctrip.xpipe.redis.core.proxy.endpoint;

/**
 * @author chen.zhu
 * <p>
 * Jun 01, 2018
 */
public class SelectOneCycle implements SelectStrategy {

    private ProxyEndpointSelector selector;

    public SelectOneCycle(ProxyEndpointSelector selector) {
        this.selector = selector;
    }

    @Override
    public boolean select() {
        return selector.selectCounts() < selector.getCandidates().size();
    }

}
