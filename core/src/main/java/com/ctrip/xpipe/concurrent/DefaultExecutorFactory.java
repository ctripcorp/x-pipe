package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.concurrent.ExecutorFactory;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import java.util.concurrent.*;

/**
 * @author wenchao.meng
 *         <p>
 *         May 10, 2017
 */
public class DefaultExecutorFactory implements ExecutorFactory{

    private static final int DEFAULT_MAX_QUEUE_SIZE = 1 << 20;
    private static final RejectedExecutionHandler DEFAULT_HANDLER = new ThreadPoolExecutor.CallerRunsPolicy();
    private static final int DEFAULT_CORE_POOL_SIZE = OsUtils.getCpuCount();
    private static final int DEFAULT_KEEPER_ALIVE_TIME_SECONDS = 60;

    private int corePoolSize = DEFAULT_CORE_POOL_SIZE;

    private int maxPoolSize = 2 * DEFAULT_CORE_POOL_SIZE;

    private int keepAliveTime = 60;

    private TimeUnit keepAliveTimeUnit = TimeUnit.SECONDS;

    private final int maxQueueSize;

    private final BlockingQueue<Runnable> workQueue;

    private final RejectedExecutionHandler rejectedExecutionHandler;

    private boolean allowCoreThreadTimeOut = true;

    private final String threadNamePrefix;

    private ThreadFactory threadFactory;

    public DefaultExecutorFactory(String threadNamePrefix, int corePoolSize, boolean allowCoreThreadTimeOut){
        this(threadNamePrefix, corePoolSize, allowCoreThreadTimeOut,
                DEFAULT_MAX_QUEUE_SIZE, 60, TimeUnit.SECONDS, DEFAULT_HANDLER);
    }

    public DefaultExecutorFactory(String threadNamePrefix, int corePoolSize, boolean allowCoreThreadTimeOut,
                                  int maxQueueSize, int keepAliveTime, TimeUnit keepAliveTimeUnit, RejectedExecutionHandler rejectedExecutionHandler){
        this.threadNamePrefix = threadNamePrefix;
        this.corePoolSize = corePoolSize;
        this.allowCoreThreadTimeOut = allowCoreThreadTimeOut;
        this.maxQueueSize = maxQueueSize;
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeUnit = keepAliveTimeUnit;
        this.workQueue = new LinkedBlockingDeque<>(maxQueueSize);
        this.rejectedExecutionHandler = rejectedExecutionHandler;
    }

    public static DefaultExecutorFactory createAllowCoreTimeout(String threadNamePrefix){

        return new DefaultExecutorFactory(threadNamePrefix, DEFAULT_CORE_POOL_SIZE, true);
    }

    public static DefaultExecutorFactory createAllowCoreTimeout(String threadNamePrefix, int corePoolSize){

        return new DefaultExecutorFactory(threadNamePrefix, corePoolSize, true);
    }

    public static DefaultExecutorFactory createAllowCoreTimeout(String threadNamePrefix, int corePoolSize, int keepAliveTimeSeconds){

        return new DefaultExecutorFactory(threadNamePrefix,
                corePoolSize,
true,
                DEFAULT_MAX_QUEUE_SIZE,
                keepAliveTimeSeconds, TimeUnit.SECONDS, DEFAULT_HANDLER);
    }

    public static DefaultExecutorFactory createAllowCoreTimeoutAbortPolicy(String threadNamePrefix){

        return new DefaultExecutorFactory(threadNamePrefix,
                DEFAULT_CORE_POOL_SIZE,
                true,
                DEFAULT_MAX_QUEUE_SIZE,
                DEFAULT_KEEPER_ALIVE_TIME_SECONDS, TimeUnit.SECONDS, new ThreadPoolExecutor.AbortPolicy());
    }

    public static DefaultExecutorFactory createAllowCoreTimeoutAbortPolicy(String threadNamePrefix, int corePoolSize){

        return new DefaultExecutorFactory(threadNamePrefix,
                corePoolSize,
                true,
                DEFAULT_MAX_QUEUE_SIZE,
                DEFAULT_KEEPER_ALIVE_TIME_SECONDS, TimeUnit.SECONDS, new ThreadPoolExecutor.AbortPolicy());
    }


    public static DefaultExecutorFactory createAllowCoreTimeoutAbortPolicy(String threadNamePrefix, int corePoolSize, int keepAliveTimeSeconds){

        return new DefaultExecutorFactory(threadNamePrefix,
                corePoolSize,
                true,
                DEFAULT_MAX_QUEUE_SIZE,
                keepAliveTimeSeconds, TimeUnit.SECONDS, new ThreadPoolExecutor.AbortPolicy());
    }


    @Override
    public ExecutorService createExecutorService() {

        int useMaxPoolSize = Math.max(corePoolSize, maxPoolSize);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                        corePoolSize, useMaxPoolSize, keepAliveTime,
                keepAliveTimeUnit, workQueue,
                threadFactory != null ? threadFactory : XpipeThreadFactory.create(threadNamePrefix),
                rejectedExecutionHandler);

        executor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
        return executor;
    }


}
