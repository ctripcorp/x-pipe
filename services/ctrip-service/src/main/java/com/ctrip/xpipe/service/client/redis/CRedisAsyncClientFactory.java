package com.ctrip.xpipe.service.client.redis;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import credis.java.client.AbstractAsyncConfig;
import credis.java.client.ClientType;
import credis.java.client.async.applier.AsyncApplierCacheProvider;
import credis.java.client.config.DefaultAsyncConfig;
import credis.java.client.config.PropertiesAware;
import credis.java.client.config.impl.DelegateClusterLevelConfig;
import credis.java.client.config.impl.PropertiesDecorator;
import credis.java.client.config.route.ConfigFrozenRoute;
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

    private CRedisAsyncClient doCreate(String clusterName, String subenv, ExecutorService credisNotifyExecutor) {
        DefaultHashStrategyFactory hashStrategyFactory = new DefaultHashStrategyFactory();
        AbstractAsyncConfig asyncConfig = decorateConfig(DefaultAsyncConfig.newBuilder().build(), clusterName);
        ConfigFrozenRoute route = DefaultRouteManager.create().createConfigFrozenRoute(clusterName, hashStrategyFactory, ClientType.APPLIER, subenv, asyncConfig);

        return new CRedisAsyncClient(new AsyncApplierCacheProvider(clusterName, DefaultRouteManager.create(), asyncConfig, hashStrategyFactory, route),
                new ApplierCacheProvider(clusterName, DelegateClusterLevelConfig.newBuilder().build(), route) , credisNotifyExecutor, route);
    }

    @Override
    public AsyncRedisClient getOrCreateClient(String clusterName, String subenv, ExecutorService credisNotifyExecutor) throws CRedisException {
        return doCreate(clusterName, subenv, credisNotifyExecutor);
    }

    @Override
    public AsyncRedisClient createClient(String clusterName, String subenv, ExecutorService credisNotifyExecutor) throws CRedisException {
        return doCreate(clusterName, subenv, credisNotifyExecutor);
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
