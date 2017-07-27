package com.ctrip.xpipe.redis.meta.server.dcchange.exception;

import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class RedisReadonlyException extends MetaServerException{

    public RedisReadonlyException(String message) {
        super(message);
    }

    public RedisReadonlyException(String message, Throwable th) {
        super(message, th);
    }

}
