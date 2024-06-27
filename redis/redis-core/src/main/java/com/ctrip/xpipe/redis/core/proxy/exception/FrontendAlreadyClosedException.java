package com.ctrip.xpipe.redis.core.proxy.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author lishanglin
 * date 2024/6/24
 */
public class FrontendAlreadyClosedException extends XpipeRuntimeException {

    public FrontendAlreadyClosedException(String message) {
        super(message);
    }

}
