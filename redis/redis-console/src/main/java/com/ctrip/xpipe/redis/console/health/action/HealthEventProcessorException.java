package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class HealthEventProcessorException extends XpipeException{

    public HealthEventProcessorException(String message){
        super(message);
    }

    public HealthEventProcessorException(String message, Throwable th) {
        super(message, th);
    }
}
