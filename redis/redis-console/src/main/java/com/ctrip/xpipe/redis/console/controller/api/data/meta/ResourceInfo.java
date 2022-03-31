package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author lishanglin
 * date 2022/3/31
 */
public class ResourceInfo {

    public int migExecThreadActive;

    public int migExecThreadMax;

    public int migPrepareThreadActive;

    public int migPrepareThreadMax;

    public int migIoCallbackThreadActive;

    public int migIoCallbackThreadMax;

    public int migExecQueueSize;

    public int migExecQueueCapacity;

    public int migPrepareQueueSize;

    public int migPrepareQueueCapacity;

    public int migIoCallbackQueueSize;

    public int migIoCallbackQueueCapacity;

    public void collectDataFromMigrationExecutor(ThreadPoolExecutor executor) {
        if (null == executor) return;
        migExecThreadActive = executor.getActiveCount();
        migExecThreadMax = executor.getMaximumPoolSize();
        BlockingQueue<Runnable> queue = executor.getQueue();
        migExecQueueSize = queue.size();
        migExecQueueCapacity = queue.size() + queue.remainingCapacity();
    }

    public void collectDataFromPrepareExecutor(ThreadPoolExecutor executor) {
        if (null == executor) return;
        migPrepareThreadActive = executor.getActiveCount();
        migPrepareThreadMax = executor.getMaximumPoolSize();
        BlockingQueue<Runnable> queue = executor.getQueue();
        migPrepareQueueSize = queue.size();
        migPrepareQueueCapacity = queue.size() + queue.remainingCapacity();
    }

    public void collectDataFromIoCallbackExecutor(ThreadPoolExecutor executor) {
        if (null == executor) return;
        migIoCallbackThreadActive = executor.getActiveCount();
        migIoCallbackThreadMax = executor.getMaximumPoolSize();
        BlockingQueue<Runnable> queue = executor.getQueue();
        migIoCallbackQueueSize = queue.size();
        migIoCallbackQueueCapacity = queue.size() + queue.remainingCapacity();
    }

}
