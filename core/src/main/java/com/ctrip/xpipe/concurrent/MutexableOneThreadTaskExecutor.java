package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.command.Tagged;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * @author chen.zhu
 * <p>
 * Jan 20, 2020
 */
public class MutexableOneThreadTaskExecutor extends OneThreadTaskExecutor {

    private Set<String> privilege = Sets.newConcurrentHashSet();

    public MutexableOneThreadTaskExecutor(Executor executors) {
        super(executors);
    }

    public MutexableOneThreadTaskExecutor(RetryCommandFactory<?> retryCommandFactory, Executor executors) {
        super(retryCommandFactory, executors);
    }

    public void executeCommand(Command<?> command) {
        super.executeCommand(command);
    }

    @SuppressWarnings("unchecked")
    public void executeMutexableCommand(Command<?> command) {
        logger.warn("[mutexExecuteCommand] {}", command);
        blockOn(command);
        command.future().addListener((CommandFutureListener) commandFuture -> {
            unblock();
        });
        executeCommand(command);
    }


    private void blockOn(Command<?> command) {
        synchronized (this) {
            block();
        }
        clear();
    }

    private void block() {

    }

    private void unblock() {

    }

    private void clear() {

    }

}
