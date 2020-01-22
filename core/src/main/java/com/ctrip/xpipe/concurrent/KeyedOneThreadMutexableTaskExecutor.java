package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.command.Command;

import java.util.concurrent.Executor;

/**
 * @author chen.zhu
 * <p>
 * Jan 22, 2020
 */
public class KeyedOneThreadMutexableTaskExecutor<K> extends KeyedOneThreadTaskExecutor<K> {

    public KeyedOneThreadMutexableTaskExecutor(Executor executors) {
        super(executors);
    }

    protected MutexableOneThreadTaskExecutor createTaskExecutor() {
        return new MutexableOneThreadTaskExecutor(executors);
    }

    public void clearAndExecute(K key, Command<?> command){

        MutexableOneThreadTaskExecutor oneThreadTaskExecutor = (MutexableOneThreadTaskExecutor) getOrCreate(key);
        oneThreadTaskExecutor.clearAndExecuteCommand(command);
    }
}
