package com.ctrip.xpipe.redis.meta.server.cluster.task;

import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class ShardingException extends MetaServerException{

    public ShardingException(String msg) {
        super(msg);
    }

    public ShardingException(String msg, Throwable th) {
        super(msg, th);
    }
}
