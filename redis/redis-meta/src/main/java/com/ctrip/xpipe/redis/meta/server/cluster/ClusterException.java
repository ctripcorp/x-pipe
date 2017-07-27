package com.ctrip.xpipe.redis.meta.server.cluster;

import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class ClusterException extends MetaServerException{
    public ClusterException(String message) {
        super(message);
    }

    public ClusterException(String message, Throwable th) {
        super(message, th);
    }
}
