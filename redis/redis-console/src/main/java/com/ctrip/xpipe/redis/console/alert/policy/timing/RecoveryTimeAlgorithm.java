package com.ctrip.xpipe.redis.console.alert.policy.timing;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface RecoveryTimeAlgorithm {
    long calculate(long checkInterval);
}
