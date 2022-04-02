package com.ctrip.xpipe.redis.meta.server.rest.data;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author lishanglin
 * date 2022/4/1
 */
public class ResourceInfo {

    public int httpThreads;

    public int httpThreadMax;

    public int httpQueueSize;

    public int httpQueueCapacity;

    public int replAdjustThreads;

    public int replAdjustThreadMax;

    public int replAdjustQueueSize;

    public int replAdjustQueueCapacity;

    public void collectDataFromReplAdjustExecutor(ThreadPoolExecutor executor) {
        if (null == executor) return;
        replAdjustThreads = executor.getPoolSize();
        replAdjustThreadMax = executor.getMaximumPoolSize();
        BlockingQueue<Runnable> queue = executor.getQueue();
        replAdjustQueueSize = queue.size();
        replAdjustQueueCapacity = queue.size() + queue.remainingCapacity();
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
