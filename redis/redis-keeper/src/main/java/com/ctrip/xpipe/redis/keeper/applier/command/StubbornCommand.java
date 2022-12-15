package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Feb 07, 2022 9:50 PM
 */
public class StubbornCommand<V> extends AbstractCommand<V> implements Command<V> {

    private final Command<V> inner;

    private final Executor retryExecutor;

    public StubbornCommand(Command<V> inner) {
        this(inner, MoreExecutors.directExecutor());
    }

    public StubbornCommand(Command<V> inner, Executor retryExecutor) {
        this.inner = inner;
        this.retryExecutor = retryExecutor;
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
                TimeUnit.MILLISECONDS.sleep(2000);
                retryExecutor.execute(this::executeTilSuccess);
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
