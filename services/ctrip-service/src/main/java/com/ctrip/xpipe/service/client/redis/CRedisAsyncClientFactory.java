package com.ctrip.xpipe.service.client.redis;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import credis.java.client.AsyncCacheFactory;
import credis.java.client.exception.CRedisException;
import credis.java.client.util.CacheFactory;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 3:05 PM
 */
public class CRedisAsyncClientFactory implements AsyncRedisClientFactory {

    @Override
    public AsyncRedisClient getOrCreateClient(String clusterName) throws CRedisException {

        return new CRedisAsyncClient(
                AsyncCacheFactory.getInstance().getOrCreateProvider(clusterName),
                CacheFactory.getProvider(clusterName));
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
