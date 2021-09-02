package com.ctrip.xpipe.redis.console.sentinel.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author lishanglin
 * date 2021/9/2
 */
public class NoSentinelsToUseException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public NoSentinelsToUseException(String msg) {
        super(msg);
    }

}
