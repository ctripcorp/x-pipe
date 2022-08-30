package com.ctrip.xpipe.service.client.redis;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import credis.java.client.async.applier.AsyncApplierProviderFactory;
import credis.java.client.exception.CRedisException;
import credis.java.client.sync.applier.ApplierFactory;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 3:05 PM
 */
public class CRedisAsyncClientFactory implements AsyncRedisClientFactory {

    @Override
    public AsyncRedisClient getOrCreateClient(String clusterName) throws CRedisException {

        return new CRedisAsyncClient(
                AsyncApplierProviderFactory.getInstance().getOrCreateProvider(clusterName),
                ApplierFactory.getProvider(clusterName));
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
