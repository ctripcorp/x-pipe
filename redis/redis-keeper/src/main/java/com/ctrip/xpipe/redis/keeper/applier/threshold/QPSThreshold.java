package com.ctrip.xpipe.redis.keeper.applier.threshold;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Dec 09, 2022 11:39
 */
public class QPSThreshold extends AbstractThreshold {

    private static final Logger logger = LoggerFactory.getLogger(QPSThreshold.class);

    private boolean printable = false;

    private String name = null;

    public QPSThreshold(long qps, ScheduledExecutorService scheduled) {
        super(qps);
        scheduled.scheduleAtFixedRate(super::reset, 1, 1, TimeUnit.SECONDS);
    }

    public QPSThreshold(long qps, ScheduledExecutorService scheduled, boolean printable, String name) {
        super(qps);
        scheduled.scheduleAtFixedRate(this::reset, 1, 1, TimeUnit.SECONDS);
        this.printable = printable;
        this.name = name;
    }

    @Override
    protected void reset() {
        if (printable) {
            logger.info("[QPSThreshold][{}]current qps - {}", (name == null ? "" : name), accumulated.get());
        }

        accumulated.set(0);
        checkOpenGate(0);
    }

    public void tryPass() {
        super.tryPass(1);
    }
}
