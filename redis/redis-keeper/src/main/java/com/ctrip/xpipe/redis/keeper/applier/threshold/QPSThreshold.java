package com.ctrip.xpipe.redis.keeper.applier.threshold;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Dec 09, 2022 11:39
 */
public class QPSThreshold extends AbstractThreshold {

    public static long ACCURACY = 10;

    public QPSThreshold(long qps, ScheduledExecutorService scheduled) {
        super(qps/ACCURACY);
        scheduled.scheduleAtFixedRate(super::reset, 1000/ACCURACY, 1000/ACCURACY, TimeUnit.MILLISECONDS);
    }

    public void tryPass() {
        super.tryPass(1);
    }
}
