package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 01, 2017
 */
@Configuration
public class MigrationResources {

    public static final String MIGRATION_EXECUTOR = "MIGRATION_EXECUTOR";

    public static final String MIGRATION_PREPARE_EXECUTOR = "MIGRATION_PREPARE_EXECUTOR";

    public static final int maxExecuteThreads = 768;

    public static final int maxPrepareThreads = 128;

    @Bean(name = MIGRATION_EXECUTOR)
    public ExecutorService getMigrationlExecutor() {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(maxExecuteThreads,
                maxExecuteThreads,
                120L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxExecuteThreads/2),
                XpipeThreadFactory.create(MIGRATION_EXECUTOR),
                new ThreadPoolExecutor.CallerRunsPolicy());
        poolExecutor.allowCoreThreadTimeOut(true);
        return MoreExecutors.getExitingExecutorService(
                poolExecutor,
                AbstractSpringConfigContext.THREAD_POOL_TIME_OUT, TimeUnit.SECONDS);
    }

    @Bean(name = MIGRATION_PREPARE_EXECUTOR)
    public ExecutorService getMigrationlPrepareExecutor() {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(maxPrepareThreads,
                maxPrepareThreads,
                120L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxExecuteThreads),
                XpipeThreadFactory.create(MIGRATION_PREPARE_EXECUTOR),
                new ThreadPoolExecutor.AbortPolicy());
        poolExecutor.allowCoreThreadTimeOut(true);
        return MoreExecutors.getExitingExecutorService(
                poolExecutor,
                AbstractSpringConfigContext.THREAD_POOL_TIME_OUT, TimeUnit.SECONDS);
    }

}
