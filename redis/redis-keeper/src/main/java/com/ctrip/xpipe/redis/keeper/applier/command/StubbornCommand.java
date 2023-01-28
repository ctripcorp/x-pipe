package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.monitor.EventMonitor;
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

    private int retryTimes;

    public StubbornCommand(Command<V> inner) {
        this(inner, MoreExecutors.directExecutor());
    }

    public StubbornCommand(Command<V> inner, Executor retryExecutor) {
        this(inner, retryExecutor, 180 /* 6 min */);
    }

    public StubbornCommand(Command<V> inner, Executor retryExecutor, int retryTimes) {
        this.inner = inner;
        this.retryExecutor = retryExecutor;
        this.retryTimes = retryTimes;
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
                    getLogger().error("UNLIKELY - setSuccess", unlikely);
                }
            } else {
                retryTimes --;
                if (retryTimes < 0) {
                    getLogger().error("[{}] failed, retry too many times, stop retrying..", this, f.cause());
                    EventMonitor.DEFAULT.logAlertEvent("drop command: " + this);

                    try {
                        future().setSuccess(null);
                    } catch (Exception unlikely) {
                        getLogger().error("UNLIKELY - setSuccess", unlikely);
                    }
                    return;
                }

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
