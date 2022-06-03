package com.ctrip.xpipe.redis.keeper.applier.sequence.mocks;

import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Feb 07, 2022 10:13 PM
 */
public class TestSleepCommand extends AbstractThreadSwitchCommand<String> {

    private static final String SUCCESS = "OK";

    protected long duration = 0L;

    public long startTime = 0L;

    public long endTime = 0L;

    public TestSleepCommand(long duration) {
        this.duration = duration;
    }

    @Override
    protected void doBusiness() {

        try {
            getLogger().info("[begin sleep] {}", this);
            startTime = System.currentTimeMillis();
            TimeUnit.MILLISECONDS.sleep(duration);
            getLogger().info("[end sleep] {}", this);
            endTime = System.currentTimeMillis();
        } catch (InterruptedException ignore) {
        }

        future().setSuccess(SUCCESS);
    }

    @Override
    public String toString() {
        return "TestSleepCommand{" +
                "duration=" + duration +
                '}';
    }
}
