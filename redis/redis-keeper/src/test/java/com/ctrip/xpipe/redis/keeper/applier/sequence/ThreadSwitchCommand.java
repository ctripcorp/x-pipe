package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.command.AbstractCommand;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Jan 31, 2022 5:53 PM
 */
public class ThreadSwitchCommand extends AbstractCommand<String> {

    private static final String SUCCESS = "success";

    private final Executor another = Executors.newSingleThreadExecutor();

    private long duration = 0L;

    public ThreadSwitchCommand() {
    }

    public ThreadSwitchCommand(long duration) {
        this.duration = duration;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    protected void doExecute() throws Throwable {

        another.execute(()->{

            try {
                TimeUnit.MILLISECONDS.sleep(duration);
            } catch (InterruptedException ignore) {
            }

            future().setSuccess(SUCCESS);
        });
    }

    @Override
    protected void doReset() {

    }
}
