package com.ctrip.xpipe.redis.core.proxy.endpoint;

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
        int index = random.nextInt(endpoints.size());
        return endpoints.get(index);
    }
}
