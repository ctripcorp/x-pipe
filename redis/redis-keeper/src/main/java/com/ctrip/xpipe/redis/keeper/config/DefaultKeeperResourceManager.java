package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.redis.core.proxy.endpoint.NextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.resource.KeeperProxyResourceManager;
import com.ctrip.xpipe.utils.LeakyBucket;

/**
 * @author chen.zhu
 * <p>
 * Feb 19, 2020
 */
public class DefaultKeeperResourceManager extends KeeperProxyResourceManager implements KeeperResourceManager {

    private LeakyBucket leakyBucket;

    public DefaultKeeperResourceManager(ProxyEndpointManager endpointManager, NextHopAlgorithm algorithm,
                                        LeakyBucket leakyBucket) {
        super(endpointManager, algorithm);
        this.leakyBucket = leakyBucket;
    }

    @Override
    public LeakyBucket getLeakyBucket() {
        return leakyBucket;
    }
}
