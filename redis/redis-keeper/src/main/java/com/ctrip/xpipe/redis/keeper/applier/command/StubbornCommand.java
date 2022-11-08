package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;

import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Feb 07, 2022 9:50 PM
 */
public class StubbornCommand<V> extends AbstractCommand<V> implements Command<V> {

    private final Command<V> inner;

    public StubbornCommand(Command<V> inner) {
        this.inner = inner;
    }

    @Override
    public String getName() {
        return "stubborn: " + inner.getName();
    }

    private void executeTilSuccess() {
        CommandFuture<V> future = inner.execute();

        future.addListener((f)->{
            if (f.isSuccess()) {
                try {
                    future().setSuccess(f.get());
                } catch (Exception unlikely) {
                    getLogger().warn("UNLIKELY - setSuccess", unlikely);
                }
            } else {
                getLogger().warn("[{}] failed, retry", this, f.cause());
                inner.reset();
                TimeUnit.MILLISECONDS.sleep(100);
                executeTilSuccess();
            }
        });
    }

    @Override
    protected void doExecute() throws Throwable {
        executeTilSuccess();
    }

    @Override
    protected void doReset() {

    }
}
