package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.exception.CommandNotExecuteException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * Jan 20, 2020
 */
public class MutexableOneThreadTaskExecutor extends OneThreadTaskExecutor {

    private AtomicReference<Command<?>> reference = new AtomicReference<>(null);

    public MutexableOneThreadTaskExecutor(Executor executors) {
        super(executors);
    }

    public MutexableOneThreadTaskExecutor(RetryCommandFactory<?> retryCommandFactory, Executor executors) {
        super(retryCommandFactory, executors);
    }

    public void executeCommand(Command<?> command) {
        synchronized (this) {
            if (reference.get() != null) {
                throw new IllegalStateException("[MutexableOneThreadTaskExecutor] blocking mode, not accept commands");
            }
        }
        super.executeCommand(command);
    }

    @SuppressWarnings("unchecked")
    public void clearAndExecuteCommand(Command<?> command) {
        logger.warn("[mutexExecuteCommand] {}", command);
        clearAndBlockOn(command);
        command.future().addListener((CommandFutureListener) commandFuture -> {
            unblock();
        });
        super.executeCommand(command);
    }

    protected Command<?> retryCommand(Command<?> command) {
        return command;
    }

    private void clearAndBlockOn(Command<?> command) {
        synchronized (this) {
            if (!tryBlock(command)) {
                return;
            }
        }
        clear();
    }

    private boolean tryBlock(Command<?> command) {
        if(!reference.compareAndSet(null, command)) {
            logger.warn("[tryBlock] command already exist");
            return false;
        }
        return true;
    }

    private void unblock() {
        reference.set(null);
    }

    private void clear() {
        List<Command<?>> commands = null;
        synchronized (this) {
            commands = Lists.newArrayList(tasks);
            tasks.clear();
        }
        Command<?> current = getCurrentCommand();
        if (current != null && !current.future().isDone()) {
            try {
                current.future().setFailure(new CommandNotExecuteException("[OneThreadExecutor][cancel running command]"));
            } catch (Exception e) {
                logger.error("[clear][cancel running commands]", e);
            }
        }
        if (commands.isEmpty()) {
            return;
        }
        commands.forEach(task -> {
            try {
                if (!task.future().isDone()) {
                    task.future().setFailure(new CommandNotExecuteException("[OneThreadExecutor][drop]"));
                }
            } catch (Exception e) {
                logger.error("[clear][cancel queued commands]", e);
            }
        });
    }

}
