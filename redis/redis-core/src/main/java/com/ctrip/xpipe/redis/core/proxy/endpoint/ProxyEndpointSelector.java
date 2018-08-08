package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.exception.NoResourceException;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 24, 2018
 */
public interface ProxyEndpointSelector {

    int selectCounts();

    ProxyEndpoint nextHop() throws NoResourceException;

    List<ProxyEndpoint> getCandidates();

    void setNextHopAlgorithm(NextHopAlgorithm algorithm);

    void setSelectStrategy(SelectStrategy strategy);
}
