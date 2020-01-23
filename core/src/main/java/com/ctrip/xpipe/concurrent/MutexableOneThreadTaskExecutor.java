package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.exception.CommandNotExecuteException;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Jan 20, 2020
 */
public class MutexableOneThreadTaskExecutor extends OneThreadTaskExecutor {

    private ScheduledExecutorService scheduled;

    public MutexableOneThreadTaskExecutor(Executor executors, ScheduledExecutorService scheduled) {
        super(executors);
        this.scheduled = scheduled;
    }

    @Override
    public void executeCommand(Command<?> command) {
        if (command instanceof RequestResponseCommand) {
            super.executeCommand(command);
        } else {
            throw new IllegalArgumentException("Only timeout enabled command is allowed");
        }
    }


    @SuppressWarnings("unchecked")
    public void clearAndExecuteCommand(RequestResponseCommand<?> command) {
        logger.info("[clearAndExecuteCommand] {}", command);
        clear();
        super.executeCommand(command);
    }

    protected Command<?> retryCommand(Command<?> command) {
        return command;
    }

    private void clear() {
        List<Command<?>> commands = null;
        synchronized (this) {
            commands = Lists.newArrayList(tasks);
            tasks.clear();
        }
        waitForCurrentTask();
        if (commands.isEmpty()) {
            return;
        }
        commands.forEach(task -> {
            try {
                if (!task.future().isDone()) {
                    synchronized (this) {
                        if (!task.future().isDone()) {
                            logger.warn("[CLEAR] {}", task.getName());
                            task.future().setFailure(new CommandNotExecuteException("[OneThreadExecutor][drop]"));
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("[clear][cancel queued commands]", e);
            }
        });
    }

    private void waitForCurrentTask() {
        RequestResponseCommand<?> current = (RequestResponseCommand<?>) getCurrentCommand();
        if (current != null && !current.future().isDone()) {
            scheduled.schedule(new Runnable() {
                @Override
                public void run() {
                    cancelLongTermRunningTask(current);
                }
            }, current.getCommandTimeoutMilli(), TimeUnit.MILLISECONDS);
        }
    }

    private void cancelLongTermRunningTask(RequestResponseCommand<?> command) {
        if (command != null && !command.future().isDone()) {
            try {
                synchronized (command.future()) {
                    if (!command.future().isDone()) {
                        command.future().setFailure(new CommandTimeoutException("[OneThreadExecutor][too long time][cancel running command]"));
                    }
                }
            } catch (Exception e) {
                logger.error("[clear][cancel running commands]", e);
            }
        }
    }

}
