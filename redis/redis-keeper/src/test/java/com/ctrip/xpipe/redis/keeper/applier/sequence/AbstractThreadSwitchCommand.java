package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.command.AbstractCommand;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author Slight
 * <p>
 * Jan 31, 2022 5:53 PM
 */
public abstract class AbstractThreadSwitchCommand<V> extends AbstractCommand<V> {

    private final Executor another = Executors.newSingleThreadExecutor();

    @Override
    public String getName() {
        return null;
    }

    @Override
    protected void doExecute() throws Throwable {

        another.execute(this::doBusiness);
    }

    protected abstract void doBusiness();

    @Override
    protected void doReset() {

    }
}
