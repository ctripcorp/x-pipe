package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class HealthEventProcessorException extends XpipeException {

    public HealthEventProcessorException(String message){
        super(message);
    }

    public HealthEventProcessorException(String message, Throwable th) {
        super(message, th);
    }
}
