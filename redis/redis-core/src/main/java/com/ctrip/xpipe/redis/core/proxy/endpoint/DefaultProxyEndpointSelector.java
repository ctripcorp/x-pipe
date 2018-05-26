package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 24, 2018
 */
public class DefaultProxyEndpointSelector implements ProxyEndpointSelector {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyEndpointSelector.class);

    private ProxyEndpointManager endpointManager;

    private List<ProxyEndpoint> candidates;

    private List<ProxyEndpoint> selected;

    private NextHopAlgorithm algorithm;

    public DefaultProxyEndpointSelector(List<ProxyEndpoint> candidates, ProxyEndpointManager endpointManager) {
        this.candidates = candidates;
        this.endpointManager = endpointManager;
        this.endpointManager.storeProxyEndpoints(candidates);
        this.selected = Lists.newArrayListWithCapacity(candidates.size());
    }

    @Override
    public ProxyEndpoint nextHop() {

        candidates.retainAll(endpointManager.getAvailableProxyEndpoints());
        candidates.removeAll(selected);

        ProxyEndpoint endpoint = algorithm.nextHop(candidates);
        if(endpoint == null) {
            logger.info("[getNextHop] no endpoint selected, chose first for default");
            endpoint = candidates.get(0);
        }
        selected.add(endpoint);

        return endpoint;
    }

    @Override
    public void setNextHopAlgorithm(NextHopAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public int selectCounts() {
        return selected.size();
    }

    @Override
    public List<ProxyEndpoint> getCandidates() {
        return candidates;
    }
}
