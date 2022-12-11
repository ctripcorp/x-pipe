package com.ctrip.xpipe.redis.keeper.applier.threshold;

/**
 * @author Slight
 * <p>
 * Dec 09, 2022 11:39
 */
public interface Threshold {

    void tryPass(long quantity);

    void release(long quantity);
}
