package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.OsUtils;
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

    public static final int maxThreads = 512;

    @Bean(name = MIGRATION_EXECUTOR)
    public ExecutorService getMigrationlExecutor() {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(maxThreads,
                maxThreads,
                120L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxThreads/2),
                XpipeThreadFactory.create(MIGRATION_EXECUTOR),
                new ThreadPoolExecutor.CallerRunsPolicy());
        poolExecutor.allowCoreThreadTimeOut(true);
        return MoreExecutors.getExitingExecutorService(
                poolExecutor,
                AbstractSpringConfigContext.THREAD_POOL_TIME_OUT, TimeUnit.SECONDS);
    }
}
