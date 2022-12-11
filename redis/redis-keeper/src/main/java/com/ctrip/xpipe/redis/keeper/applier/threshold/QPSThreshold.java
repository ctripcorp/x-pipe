package com.ctrip.xpipe.redis.keeper.applier.threshold;

/**
 * @author Slight
 * <p>
 * Dec 09, 2022 11:39
 */
public class QPSThreshold extends AbstractThreshold {

    public QPSThreshold(long limit) {
        super(limit);
    }
}
