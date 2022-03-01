package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * @author Slight
 *
 * Make sure to execute this command on stateThread.
 * Jan 31, 2022 1:06 PM
 */
public class SequenceCommand<V> extends AbstractCommand<V> implements Command<V> {

    private final List<SequenceCommand<?>> pasts;

    private int complete = 0;

    private final Command<V> inner;

    private final Executor stateThread;

    private final Executor workerThreads;

    public SequenceCommand(Command<V> inner, Executor stateThread, Executor workerThreads) {
        this(Lists.newArrayList(), inner, stateThread, workerThreads);
    }

    public SequenceCommand(SequenceCommand<?> past, Command<V> inner, Executor stateThread, Executor workerThreads) {
        this(Lists.newArrayList(past), inner, stateThread, workerThreads);
    }

    public SequenceCommand(List<SequenceCommand<?>> pasts, Command<V> inner, Executor stateThread, Executor workerThreads) {
        this.pasts = pasts;
        this.inner = inner;
        this.stateThread = stateThread;
        this.workerThreads = workerThreads;
    }

    private void executeSelf() {
        CommandFuture<V> future = inner.execute(workerThreads);
        future.addListener((f)->{
            if (f.isSuccess()) {
                stateThread.execute(() -> {
                    try {
                        future().setSuccess(f.get());
                    } catch (Exception unlikely) {
                        getLogger().warn("UNLIKELY - setSuccess", unlikely);
                    }
                });
            } else {
                getLogger().warn("[executeSelf] yet UNLIKELY - stubborn command will retry util success.");
                stateThread.execute(() -> {
                    future().setFailure(f.cause());
                });
            }
        });
    }

    private void nextAfter(List<SequenceCommand<?>> pasts) {
        for (SequenceCommand<?> past : pasts) {
            past.future().addListener((f)->{
                if (f.isSuccess()) {
                    if (++complete == pasts.size()) {
                        executeSelf();
                    }
                } else {
                    getLogger().warn("[nextAfter] yet UNLIKELY - stubborn command will retry util success.");
                    future().setFailure(f.cause());
                }
            });
        }
    }

    @Override
    protected void doExecute() throws Throwable {
        if (pasts == null || pasts.size() == 0) {
            executeSelf();
            return;
        }

        nextAfter(pasts);
    }

    @Override
    public String getName() {
        return "sequence: " + inner.getName();
    }

    @Override
    protected void doReset() {
        inner.reset();
    }
}