package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

import java.time.Duration;
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

    public static final String MIGRATION_IO_CALLBACK_EXECUTOR = "MIGRATION_IO_CALLBACK_EXECUTOR";

    public static final String MIGRATION_HTTP_LOOP_RESOURCE = "MIGRATION_HTTP_LOOP_RESOURCE";

    public static final String MIGRATION_HTTP_CONNECTION_PROVIDER = "MIGRATION_HTTP_CONNECTION_PROVIDER";

    public static final int maxExecuteThreads = 768;

    public static final int maxPrepareThreads = 128;

    public static final int maxIOCallbackThreads = 100;

    public static final String BI_DIRECTION_MIGRATION_EXECUTOR = "BI_DIRECTION_MIGRATION_EXECUTOR";

    public static final int maxBiExecuteThreads = 64;

    private static final Logger logger = LoggerFactory.getLogger(MigrationResources.class);

    @Bean(name = MIGRATION_EXECUTOR)
    public ExecutorService getMigrationExecutor() {
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
    public ExecutorService getMigrationPrepareExecutor() {
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

    @Bean(name = MIGRATION_IO_CALLBACK_EXECUTOR)
    public ExecutorService getMigrationIOCallbackExecutor() {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(maxIOCallbackThreads,
                maxIOCallbackThreads,
                120L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                XpipeThreadFactory.create(MIGRATION_IO_CALLBACK_EXECUTOR),
                new ThreadPoolExecutor.DiscardPolicy() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        super.rejectedExecution(r, e);
                        logger.info("[migration-io-callback][rejectedExecution] {}", r);
                    }
                });
        poolExecutor.allowCoreThreadTimeOut(true);
        return MoreExecutors.getExitingExecutorService(
                poolExecutor,
                AbstractSpringConfigContext.THREAD_POOL_TIME_OUT, TimeUnit.SECONDS);
    }

    @Bean(name = MIGRATION_HTTP_LOOP_RESOURCE)
    public LoopResources getMigrationLoopResource() {
        LoopResources loopResources = LoopResources.create("MigrationHttpLoopResource", LoopResources.DEFAULT_IO_WORKER_COUNT, true);
        loopResources.onClient(true); // load netty native lib at first, avoid waiting on migration
        return loopResources;
    }

    @Bean(name = MIGRATION_HTTP_CONNECTION_PROVIDER)
    public ConnectionProvider getMigrationConnectionProvider() {
        return ConnectionProvider.builder("MigrationHttpConnProvider")
                .maxConnections(2000)
                .pendingAcquireTimeout(Duration.ofMillis(1000))
                .maxIdleTime(Duration.ofMillis(30000)).build();
    }

    @Bean(name = BI_DIRECTION_MIGRATION_EXECUTOR)
    public ExecutorService getBiDirectionMigrationExecutor() {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(maxBiExecuteThreads,
                maxBiExecuteThreads,
                120L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxBiExecuteThreads/2),
                XpipeThreadFactory.create(BI_DIRECTION_MIGRATION_EXECUTOR),
                new ThreadPoolExecutor.AbortPolicy());
        poolExecutor.allowCoreThreadTimeOut(true);
        return MoreExecutors.getExitingExecutorService(
                poolExecutor,
                AbstractSpringConfigContext.THREAD_POOL_TIME_OUT, TimeUnit.SECONDS);
    }

}
