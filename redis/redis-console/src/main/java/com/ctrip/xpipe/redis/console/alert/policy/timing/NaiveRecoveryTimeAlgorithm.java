package com.ctrip.xpipe.redis.console.alert.policy.timing;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public class NaiveRecoveryTimeAlgorithm implements RecoveryTimeAlgorithm {

    private static final long delta = 1000 * 10;

    @Override
    public long calculate(long checkInterval) {
        return checkInterval * 2 + delta;
    }
}
