package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.exception.NoResourceException;
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

    private SelectStrategy strategy;

    public DefaultProxyEndpointSelector(List<ProxyEndpoint> candidates, ProxyEndpointManager endpointManager) {
        this.candidates = candidates;
        this.endpointManager = endpointManager;
        this.endpointManager.storeProxyEndpoints(candidates);
        this.selected = Lists.newArrayListWithCapacity(candidates.size());
    }

    @Override
    public ProxyEndpoint nextHop() throws NoResourceException {

        if(strategy != null && !strategy.select()) {
            throw new NoResourceException(String.format("No resource for strategy: %s", strategy.getClass().getSimpleName()));
        }
        logger.debug("[candidates][before]{}", candidates);
        List<ProxyEndpoint> toBeSelected = Lists.newArrayList(candidates);
        toBeSelected.retainAll(endpointManager.getAvailableProxyEndpoints());
        logger.debug("[candidates][after]{}", toBeSelected);

        ProxyEndpoint endpoint = algorithm.nextHop(toBeSelected);
        if(endpoint == null) {
            throw new NoResourceException("No candidates available");
        }
        selected.add(endpoint);

        return endpoint;
    }

    @Override
    public void setNextHopAlgorithm(NextHopAlgorithm algorithm) {
        logger.debug("[setNextHopAlgorithm] algorithm: {}", algorithm.getClass());
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

    @Override
    public void setSelectStrategy(SelectStrategy strategy) {
        logger.debug("[setSelectStrategy] strategy: {}", strategy);
        this.strategy = strategy;
    }
}
