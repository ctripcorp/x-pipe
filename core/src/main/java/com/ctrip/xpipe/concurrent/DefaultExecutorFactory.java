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

    private int corePoolSize = OsUtils.getCpuCount();

    private int maxPoolSize = 2 * OsUtils.getCpuCount();

    private int keepAliveTime = 60;

    private TimeUnit keepAliveTimeUnit = TimeUnit.SECONDS;

    private BlockingQueue<Runnable> workQueue = new LinkedBlockingDeque<>();

    private RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();

    private boolean allowCoreThreadTimeOut = true;

    private String threadNamePrefix;

    private ThreadFactory threadFactory;

    public DefaultExecutorFactory(String threadNamePrefix, int corePoolSize, boolean allowCoreThreadTimeOut){
        this(threadNamePrefix, corePoolSize, allowCoreThreadTimeOut, 60, TimeUnit.SECONDS);
    }

    public DefaultExecutorFactory(String threadNamePrefix, int corePoolSize, boolean allowCoreThreadTimeOut, int keepAliveTime, TimeUnit keepAliveTimeUnit){
        this.threadNamePrefix = threadNamePrefix;
        this.corePoolSize = corePoolSize;
        this.allowCoreThreadTimeOut = allowCoreThreadTimeOut;
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeUnit = keepAliveTimeUnit;
    }

    public static DefaultExecutorFactory createAllowCoreTimeout(String threadNamePrefix, int corePoolSize){

        return new DefaultExecutorFactory(threadNamePrefix, corePoolSize, true);
    }

    public static DefaultExecutorFactory createAllowCoreTimeout(String threadNamePrefix, int corePoolSize, int keepAliveTimeSeconds){

        return new DefaultExecutorFactory(threadNamePrefix, corePoolSize, true, keepAliveTimeSeconds, TimeUnit.SECONDS);
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
