package com.ctrip.xpipe.api.pool;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class ObjectPoolException extends XpipeException{

    public ObjectPoolException(String message) {
        super(message);
    }

    public ObjectPoolException(String message, Throwable th) {
        super(message, th);
    }
}
