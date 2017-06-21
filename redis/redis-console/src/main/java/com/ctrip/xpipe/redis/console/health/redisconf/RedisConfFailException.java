package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleRuntimeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 13, 2017
 */
public class RedisConfFailException extends RedisConsoleRuntimeException{

    public RedisConfFailException(String message) {
        super(message);
    }

    public RedisConfFailException(String msg, Throwable th) {
        super(msg, th);
    }
}
