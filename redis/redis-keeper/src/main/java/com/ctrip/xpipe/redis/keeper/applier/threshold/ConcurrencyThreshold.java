package com.ctrip.xpipe.redis.keeper.applier.threshold;

/**
 * @author Slight
 * <p>
 * Dec 14, 2022 11:44
 */
public class ConcurrencyThreshold extends AbstractThreshold {

    public ConcurrencyThreshold(long concurrency) {
        super(concurrency);
    }

    public void tryPass() {
        super.tryPass(1);
    }

    public void release() {
        super.release(1);
    }

}
