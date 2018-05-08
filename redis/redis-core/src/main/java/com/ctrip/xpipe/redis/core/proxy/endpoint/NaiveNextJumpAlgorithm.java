package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.util.List;
import java.util.Random;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class NaiveNextJumpAlgorithm implements NextJumpAlgorithm {

    private Random random = new Random();

    @Override
    public ProxyEndpoint nextJump(List<ProxyEndpoint> endpoints) {
        int index = random.nextInt(endpoints.size());
        return endpoints.get(index);
    }
}
