package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;

import java.util.concurrent.atomic.AtomicReference;

public abstract class CausalCommand<T, V> extends AbstractCommand implements Causal<CommandFuture<T>, CommandFuture<V>> {

    private AtomicReference<CommandFuture<T>> previous = new AtomicReference<>();

    @Override
    protected void doExecute() throws Exception {
        if(previous.get() == null || !previous.get().isDone()) {
            throw new CausalException("Previous Future not complete");
        }

        // do nothing, as the future is set during onSuccess/onFailure
    }

    @Override
    public CommandFuture<V> getCausation(CommandFuture<T> previousCommandFuture) {
        if(this.previous.compareAndSet(null, previousCommandFuture)) {
            previousCommandFuture.addListener(new CommandFutureListener<T>() {
                @Override
                public void operationComplete(CommandFuture<T> commandFuture) throws Exception {
                    if(commandFuture.isSuccess()) {
                        onSuccess(commandFuture.getNow());
                    } else {
                        onFailure(commandFuture.cause());
                    }
                }
            });
            return future();
        }
        logger.error("[causal failure] previous already set");
        return null;
    }

    @Override
    protected void doExecuteWhenCommandDone() {

    }

    @Override
    protected void doReset() {
        previous.set(null);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    protected abstract void onSuccess(T t);

    protected abstract void onFailure(Throwable throwable);
}
