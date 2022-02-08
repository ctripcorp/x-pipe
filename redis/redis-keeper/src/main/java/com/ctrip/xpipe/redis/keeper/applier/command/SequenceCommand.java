package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;

import java.util.concurrent.Executor;

/**
 * @author Slight
 * <p>
 * Jan 31, 2022 1:06 PM
 */
public class SequenceCommand<V> extends AbstractCommand<V> implements Command<V> {

    private final SequenceCommand<?> past;

    private final Command<V> inner;

    private final Executor singleThread;

    public SequenceCommand(Command<V> inner, Executor singleThread) {
        this(null, inner, singleThread);
    }

    public SequenceCommand(SequenceCommand<?> past, Command<V> inner, Executor singleThread) {
        this.past = past;
        this.inner = inner;
        this.singleThread = singleThread;
    }

    private void executeSelf() {
        CommandFuture<V> future = inner.execute();
        future.addListener((f)->{
            if (f.isSuccess()) {
                singleThread.execute(() -> {
                    try {
                        future().setSuccess(f.get());
                    } catch (Exception unlikely) {
                        getLogger().warn("UNLIKELY - setSuccess", unlikely);
                    }
                });
            } else {
                getLogger().warn("yet UNLIKELY - stubborn command will retry util success.");
                singleThread.execute(() -> {
                    future().setFailure(f.cause());
                });
            }
        });
    }

    private void nextAfter(CommandFuture<?> future) {
        future.addListener((f)->{
            if (f.isSuccess()) {
                executeSelf();
            } else {
                future().setFailure(f.cause());
            }
        });
    }

    @Override
    protected void doExecute() throws Throwable {
        if (past == null) {
            executeSelf();
            return;
        }

        nextAfter(past.future());
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