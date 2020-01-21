package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.command.Command;
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
public class OneThreadTaggedTaskExecutor extends OneThreadTaskExecutor {

    private Set<String> privilege = Sets.newConcurrentHashSet();

    public OneThreadTaggedTaskExecutor(Executor executors) {
        super(executors);
    }

    public OneThreadTaggedTaskExecutor(RetryCommandFactory<?> retryCommandFactory, Executor executors) {
        super(retryCommandFactory, executors);
    }

    public void executeCommand(Command<?> command) {

        logger.debug("[executeCommand][offer it in pool]{}", command);
        if (!(command instanceof Tagged)) {
            throw new IllegalArgumentException("Command should be tagged");
        }
        if (isBlocked(command)) {
            command.future().cancel(true);
            return;
        }
        super.executeCommand(command);
    }

    public OneThreadTaggedTaskExecutor clear(String... tags) {
        return this;
    }

    public OneThreadTaggedTaskExecutor clearAllExcept(String... tags) {
        if (tags == null || tags.length < 1) {
            return this;
        }
        if (tasks.isEmpty()) {
            return this;
        }
        Set<String> survivors = Sets.newHashSet(tags);
        synchronized (this) {
            tasks.removeIf(task->{
                return !survivors.contains(((Tagged) task).getTag());
            });
        }
        return this;
    }

    public OneThreadTaggedTaskExecutor blockExcept(String... tags) {
        if (tags == null || tags.length < 1) {
            return this;
        }
        privilege.addAll(Arrays.asList(tags));
        return this;
    }

    public void unblockAll() {
        privilege.clear();
    }

    private boolean isBlocked(Command<?> command) {
        if (privilege.isEmpty()) {
            return false;
        }
        String tag = ((Tagged) command).getTag();
        return !privilege.contains(tag);
    }

}
