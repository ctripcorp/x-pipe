package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class SampleException extends XpipeException{

    public SampleException(String message){
        super(message);
    }

    public SampleException(String message, Throwable th) {
        super(message, th);
    }
}
