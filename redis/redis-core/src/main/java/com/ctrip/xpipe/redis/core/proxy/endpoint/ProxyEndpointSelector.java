package com.ctrip.xpipe.redis.core.proxy.endpoint;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 24, 2018
 */
public interface ProxyEndpointSelector {

    int selectCounts();

    ProxyEndpoint nextHop();

    List<ProxyEndpoint> getCandidates();

    void setNextHopAlgorithm(NextHopAlgorithm algorithm);
}
