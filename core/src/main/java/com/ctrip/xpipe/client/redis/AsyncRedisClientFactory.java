package com.ctrip.xpipe.client.redis;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

import java.util.concurrent.ExecutorService;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 3:00 PM
 */
public interface AsyncRedisClientFactory extends Ordered {

    AsyncRedisClientFactory DEFAULT = ServicesUtil.getAsyncRedisClientFactory();

    AsyncRedisClient getOrCreateClient(String clusterName, String subenv, ExecutorService credisNotifyExecutor) throws Exception;

    AsyncRedisClient createClient(String clusterName, String subenv, ExecutorService credisNotifyExecutor) throws Exception;

    default int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
