package com.ctrip.xpipe.redis.core.proxy.resource;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.exception.NoResourceException;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.endpoint.SelectNTimes;
import com.ctrip.xpipe.redis.core.proxy.endpoint.SelectStrategy;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 06, 2018
 */
public class ConsoleProxyResourceManager implements ProxyResourceManager {

    private NextHopAlgorithm algorithm;

    public ConsoleProxyResourceManager(NextHopAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public ProxyEndpointSelector createProxyEndpointSelector(ProxyConnectProtocol protocol) {
        ProxyEndpointSelector selector = new ConsoleProxyEndpointSelector(protocol.nextEndpoints());
        selector.setNextHopAlgorithm(algorithm);
        selector.setSelectStrategy(new SelectNTimes(selector, SelectNTimes.INFINITE));
        return selector;
    }

    private static class ConsoleProxyEndpointSelector implements ProxyEndpointSelector {

        private static final Logger logger = LoggerFactory.getLogger(ConsoleProxyEndpointSelector.class);

        private List<ProxyEndpoint> candidates;

        private List<ProxyEndpoint> selected;

        private NextHopAlgorithm algorithm;

        private SelectStrategy strategy;

        public ConsoleProxyEndpointSelector(List<ProxyEndpoint> candidates) {
            this.candidates = candidates;
            this.selected = Lists.newArrayListWithCapacity(candidates.size());
        }

        @Override
        public ProxyEndpoint nextHop() throws NoResourceException {

            if(strategy != null && !strategy.select()) {
                throw new NoResourceException(String.format("No resource for strategy: %s", strategy.getClass().getSimpleName()));
            }

            ProxyEndpoint endpoint = algorithm.nextHop(candidates);
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
}
