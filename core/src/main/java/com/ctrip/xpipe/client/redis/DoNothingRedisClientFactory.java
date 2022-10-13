package com.ctrip.xpipe.client.redis;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 3:02 PM
 */
public class DoNothingRedisClientFactory implements AsyncRedisClientFactory {

    @Override
    public AsyncRedisClient getOrCreateClient(String clusterName) {
        return new DoNothingRedisClient();
    }
}
