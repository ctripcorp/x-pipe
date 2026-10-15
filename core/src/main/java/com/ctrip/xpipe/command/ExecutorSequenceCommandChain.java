package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;

import java.util.concurrent.Executor;

/**
 * A {@link SequenceCommandChain} whose stages run on the given executor rather than on whichever
 * thread completed the previous stage.
 *
 * {@link AbstractCommandChain#executeCommand} starts each stage with the direct executor, and a
 * chain resumes from the listener that completed the previous stage -- so from the second stage
 * onwards, {@code doExecute()} would run on that thread. When the stages are network commands that
 * thread is a netty event loop, which must not carry application work.
 *
 * Stages still keep their order: each one is only submitted once the previous has completed.
 */
public class ExecutorSequenceCommandChain extends SequenceCommandChain {

    private final Executor executor;

    /**
     * Fail-stop and silent: {@link SequenceCommandChain} logs every stage failure at error level,
     * but for a health check most stage failures are ordinary conditions (an INFO that does not
     * report a master, a Keeper missing from the meta). The stages log their own outcome at the
     * level it deserves, and the owner of the chain logs the aggregate failure.
     */
    public ExecutorSequenceCommandChain(Executor executor) {
        super(false, false);
        this.executor = executor;
    }

    @Override
    public CommandFuture<?> executeCommand(Command<?> command) {
        CommandFuture<?> future = command.execute(executor);
        addResult(future);
        return future;
    }
}
