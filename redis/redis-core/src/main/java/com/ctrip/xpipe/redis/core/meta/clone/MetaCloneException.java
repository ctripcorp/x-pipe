package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class MetaCloneException extends XpipeRuntimeException {

    public MetaCloneException(String message) {
        super(message);
    }

    public MetaCloneException(String message, Throwable th) {
        super(message, th);
    }

}
