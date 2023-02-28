package com.ctrip.xpipe.service.client.redis;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import credis.java.client.AbstractAsyncConfig;
import credis.java.client.async.applier.AsyncApplierCacheProvider;
import credis.java.client.config.DefaultAsyncConfig;
import credis.java.client.config.PropertiesAware;
import credis.java.client.config.impl.DelegateClusterLevelConfig;
import credis.java.client.config.impl.PropertiesDecorator;
import credis.java.client.config.route.DefaultRouteManager;
import credis.java.client.exception.CRedisException;
import credis.java.client.sync.applier.ApplierCacheProvider;
import credis.java.client.util.DefaultHashStrategyFactory;

import java.util.concurrent.ExecutorService;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 3:05 PM
 */
public class CRedisAsyncClientFactory implements AsyncRedisClientFactory {

    private CRedisAsyncClient doCreate(String clusterName, ExecutorService credisNotifyExecutor) {
        return new CRedisAsyncClient(
                new AsyncApplierCacheProvider(clusterName, DefaultRouteManager.create(),
                        decorateConfig(DefaultAsyncConfig.newBuilder().build(), clusterName), new DefaultHashStrategyFactory()),
                new ApplierCacheProvider(clusterName, DelegateClusterLevelConfig.newBuilder().build()) , credisNotifyExecutor);
    }

    @Override
    public AsyncRedisClient getOrCreateClient(String clusterName, ExecutorService credisNotifyExecutor) throws CRedisException {
        return doCreate(clusterName, credisNotifyExecutor);
    }

    @Override
    public AsyncRedisClient createClient(String clusterName, ExecutorService credisNotifyExecutor) throws CRedisException {
        return doCreate(clusterName, credisNotifyExecutor);
    }

    private AbstractAsyncConfig decorateConfig(AbstractAsyncConfig config, String clusterName) {

        if (config instanceof PropertiesAware) {
            PropertiesDecorator.getOrCreateInstance().decorate(clusterName, (PropertiesAware) config);
        }

        return config;
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
