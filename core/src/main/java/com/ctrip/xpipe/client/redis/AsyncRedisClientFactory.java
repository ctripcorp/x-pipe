package com.ctrip.xpipe.client.redis;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 3:00 PM
 */
public interface AsyncRedisClientFactory extends Ordered {

    AsyncRedisClientFactory DEFAULT = ServicesUtil.getAsyncRedisClientFactory();

    AsyncRedisClient getOrCreateClient(String clusterName) throws Exception;

    default int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
