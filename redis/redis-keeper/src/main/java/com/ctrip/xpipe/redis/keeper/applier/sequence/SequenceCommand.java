package com.ctrip.xpipe.redis.keeper.applier.sequence;

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
                        super.future.get().setSuccess(f.get());
                    } catch (Exception unlikely) {
                        getLogger().warn("UNLIKELY - setSuccess", unlikely);
                    }
                });
            } else {
                singleThread.execute(() -> {
                    super.future.get().setFailure(f.cause());
                });
            }
        });
    }

    private void retryOrNext(CommandFuture<?> future) {
        future.addListener((f)->{
            if (f.isSuccess()) {
                executeSelf();
            } else {
                f.command().reset();
                retryOrNext(f.command().execute());
            }
        });
    }

    @Override
    protected void doExecute() throws Throwable {
        if (past == null) {
            executeSelf();
            return;
        }

        retryOrNext(past.future());
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