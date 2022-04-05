package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.database.ConnectionPoolDesc;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author lishanglin
 * date 2022/3/31
 */
public class ResourceInfo {

    public int migExecThreads;

    public int migExecThreadMax;

    public int migPrepareThreads;

    public int migPrepareThreadMax;

    public int migIoCallbackThreads;

    public int migIoCallbackThreadMax;

    public int httpThreads;

    public int httpThreadMax;

    public int migExecQueueSize;

    public int migExecQueueCapacity;

    public int migPrepareQueueSize;

    public int migPrepareQueueCapacity;

    public int migIoCallbackQueueSize;

    public int migIoCallbackQueueCapacity;

    public int httpQueueSize;

    public int httpQueueCapacity;

    public ConnectionPoolDesc connectionPoolDesc;

    public void collectDataFromMigrationExecutor(ThreadPoolExecutor executor) {
        if (null == executor) return;
        migExecThreads = executor.getPoolSize();
        migExecThreadMax = executor.getMaximumPoolSize();
        BlockingQueue<Runnable> queue = executor.getQueue();
        migExecQueueSize = queue.size();
        migExecQueueCapacity = queue.size() + queue.remainingCapacity();
    }

    public void collectDataFromPrepareExecutor(ThreadPoolExecutor executor) {
        if (null == executor) return;
        migPrepareThreads = executor.getPoolSize();
        migPrepareThreadMax = executor.getMaximumPoolSize();
        BlockingQueue<Runnable> queue = executor.getQueue();
        migPrepareQueueSize = queue.size();
        migPrepareQueueCapacity = queue.size() + queue.remainingCapacity();
    }

    public void collectDataFromIoCallbackExecutor(ThreadPoolExecutor executor) {
        if (null == executor) return;
        migIoCallbackThreads = executor.getPoolSize();
        migIoCallbackThreadMax = executor.getMaximumPoolSize();
        BlockingQueue<Runnable> queue = executor.getQueue();
        migIoCallbackQueueSize = queue.size();
        migIoCallbackQueueCapacity = queue.size() + queue.remainingCapacity();
    }

    public void collectDataFromHttpExecutor(ThreadPoolExecutor executor) {
        if (null == executor) return;
        httpThreads = executor.getPoolSize();
        httpThreadMax = executor.getMaximumPoolSize();
        BlockingQueue<Runnable> queue = executor.getQueue();
        httpQueueSize = queue.size();
        httpQueueCapacity = queue.size() + queue.remainingCapacity();
    }

}
