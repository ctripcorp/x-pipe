package com.ctrip.xpipe.redis.keeper.applier.sequence;

import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Feb 07, 2022 10:13 PM
 */
public class TestSleepCommand extends AbstractThreadSwitchCommand<String> {

    private static final String SUCCESS = "success";

    private long duration = 0L;

    public TestSleepCommand(long duration) {
        this.duration = duration;
    }

    @Override
    protected void doBusiness() {

        try {
            getLogger().info("[begin sleep] {}", duration);
            TimeUnit.MILLISECONDS.sleep(duration);
            getLogger().info("[end sleep] {}", duration);
        } catch (InterruptedException ignore) {
        }

        future().setSuccess(SUCCESS);
    }
}
