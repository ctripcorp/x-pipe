package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.command.RequestResponseCommand;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Jan 22, 2020
 */
public class KeyedOneThreadMutexableTaskExecutor<K> extends KeyedOneThreadTaskExecutor<K> {

    private ScheduledExecutorService scheduled;

    public KeyedOneThreadMutexableTaskExecutor(Executor executors, ScheduledExecutorService scheduled) {
        super(executors);
        this.scheduled = scheduled;
    }

    protected MutexableOneThreadTaskExecutor createTaskExecutor() {
        return new MutexableOneThreadTaskExecutor(executors, scheduled);
    }

    public void clearAndExecute(K key, RequestResponseCommand<?> command){

        MutexableOneThreadTaskExecutor oneThreadTaskExecutor = (MutexableOneThreadTaskExecutor) getOrCreate(key);
        oneThreadTaskExecutor.clearAndExecuteCommand(command);
    }
}
