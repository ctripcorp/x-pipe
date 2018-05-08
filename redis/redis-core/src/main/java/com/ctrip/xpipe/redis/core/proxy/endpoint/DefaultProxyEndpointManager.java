package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.netty.TcpPortCheckCommand;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DefaultProxyEndpointManager implements ProxyEndpointManager {

    private Set<ProxyEndpoint> allEndpoints = Sets.newConcurrentHashSet();

    private Set<ProxyEndpoint> availableEndpoints = Sets.newConcurrentHashSet();

    private NextJumpAlgorithm algorithm;

    @Override
    public List<ProxyEndpoint> getAvailableProxyEndpoints() {
        return Lists.newArrayList(availableEndpoints);
    }

    @Override
    public List<ProxyEndpoint> getAllProxyEndpoints() {
        return Lists.newArrayList(allEndpoints);
    }

    @Override
    public ProxyEndpoint getNextJump(List<ProxyEndpoint> candidates) {

        recordProxyEndpoints(candidates);
        candidates.retainAll(getAvailableProxyEndpoints());

        ProxyEndpoint endpoint = algorithm.nextJump(candidates);
        if(endpoint == null) {
            endpoint = candidates.get(0);
        }
        return endpoint;
    }

    @Override
    public void setNextJumpAlgorithm(NextJumpAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public boolean checkConnectivity(ProxyEndpoint endpoint) {
        return false;
    }

    private void recordProxyEndpoints(List<ProxyEndpoint> candidates) {
        List<ProxyEndpoint> newArrival = Lists.newArrayList(candidates);
        newArrival.removeAll(allEndpoints);
        allEndpoints.addAll(newArrival);
        availableEndpoints.addAll(newArrival);
    }

}
