package com.ctrip.xpipe.redis.checker.alert.policy;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public interface AlertPolicy {

    boolean supports(Class<? extends AlertPolicy> clazz);
}
