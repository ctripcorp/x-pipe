package com.ctrip.xpipe.redis.keeper.applier.threshold;

/**
 * @author Slight
 * <p>
 * Dec 09, 2022 11:39
 */
public class MemoryThreshold extends AbstractThreshold {

    public MemoryThreshold(long bytes) {
        super(bytes);
    }

    @Override
    public void tryPass(long bytes) {
        super.tryPass(bytes);
    }

    @Override
    public void release(long bytes) {
        super.release(bytes);
    }
}
