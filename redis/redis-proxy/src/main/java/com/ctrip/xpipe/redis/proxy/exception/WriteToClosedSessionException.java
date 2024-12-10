package com.ctrip.xpipe.redis.proxy.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

public class WriteToClosedSessionException extends XpipeRuntimeException {

    public WriteToClosedSessionException(String message){
        super(message);
    }

    public WriteToClosedSessionException(String message, Throwable th){
        super(message, th);
    }

}
