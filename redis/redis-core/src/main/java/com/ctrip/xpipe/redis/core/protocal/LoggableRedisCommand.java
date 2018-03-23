package com.ctrip.xpipe.redis.core.protocal;

/**
 * @author chen.zhu
 * <p>
 * Mar 23, 2018
 */
public interface LoggableRedisCommand<T> extends RedisCommand<T> {
    void logResponse(boolean isResponseLoggable);

    void logRequest(boolean isRequestLoggable);
}
