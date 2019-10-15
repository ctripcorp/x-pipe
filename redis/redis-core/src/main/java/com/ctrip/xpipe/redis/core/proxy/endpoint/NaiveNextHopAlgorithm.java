package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.proxy.ProxyEndpoint;

import java.util.List;
import java.util.Random;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class NaiveNextHopAlgorithm implements NextHopAlgorithm {

    private Random random = new Random();

    @Override
    public ProxyEndpoint nextHop(List<ProxyEndpoint> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            return null;
        }
        int index = random.nextInt(endpoints.size());
        return endpoints.get(index);
    }
}
