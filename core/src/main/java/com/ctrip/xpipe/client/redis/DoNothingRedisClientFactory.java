package com.ctrip.xpipe.client.redis;

import java.util.concurrent.ExecutorService;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 3:02 PM
 */
public class DoNothingRedisClientFactory implements AsyncRedisClientFactory {

    @Override
    public AsyncRedisClient getOrCreateClient(String clusterName, String subenv, ExecutorService credisNotifyExecutor) {
        return new DoNothingRedisClient();
    }

    @Override
    public AsyncRedisClient createClient(String clusterName, String subenv, ExecutorService credisNotifyExecutor) {
        return new DoNothingRedisClient();
    }
}
