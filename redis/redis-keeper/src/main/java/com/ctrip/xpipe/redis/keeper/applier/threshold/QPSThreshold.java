package com.ctrip.xpipe.redis.keeper.applier.threshold;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Dec 09, 2022 11:39
 */
public class QPSThreshold extends AbstractThreshold {

    public QPSThreshold(long qps, ScheduledExecutorService scheduled) {
        super(qps);
        scheduled.scheduleAtFixedRate(super::reset, 1, 1, TimeUnit.SECONDS);
    }

    public void tryPass() {
        super.tryPass(1);
    }
}
