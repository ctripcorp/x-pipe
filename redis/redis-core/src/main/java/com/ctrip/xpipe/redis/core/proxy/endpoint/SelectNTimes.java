package com.ctrip.xpipe.redis.core.proxy.endpoint;

/**
 * @author chen.zhu
 * <p>
 * Jun 01, 2018
 */
public class SelectNTimes implements SelectStrategy {

    private int times;

    private ProxyEndpointSelector selector;

    public SelectNTimes(ProxyEndpointSelector selector, int times) {
        this.selector = selector;
        this.times = times;
    }

    @Override
    public boolean select() {
        return times > selector.selectCounts();
    }

}
