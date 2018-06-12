package com.ctrip.xpipe.redis.console.alert.policy.timing;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public class NaiveRecoveryTimeAlgorithm implements RecoveryTimeAlgorithm {

    @Override
    public long calculate(long checkInterval) {
        return checkInterval * 2;
    }
}
