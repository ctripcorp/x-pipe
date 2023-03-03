package com.ctrip.xpipe.redis.keeper.applier.threshold;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BytesPerSecondThreshold extends AbstractThreshold {

    public BytesPerSecondThreshold(long maxBytesOneSecond, ScheduledExecutorService scheduled) {
        super(maxBytesOneSecond);
        scheduled.scheduleAtFixedRate(this::reset, 0, 1, TimeUnit.SECONDS);
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
