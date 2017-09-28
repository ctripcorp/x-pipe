package com.ctrip.xpipe.redis.console.health.redisconf;

/**
 * @author chen.zhu
 * <p>
 * Sep 28, 2017
 */
public interface RedisConfManager {
    RedisConf findOrCreateConfig(String host, int port);
}
