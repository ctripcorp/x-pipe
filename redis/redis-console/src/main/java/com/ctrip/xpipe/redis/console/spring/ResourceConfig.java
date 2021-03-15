package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.netty.ProxyEnabledNettyKeyedPoolClientFactory;
import com.ctrip.xpipe.redis.core.proxy.resource.ConsoleProxyResourceManager;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;

import static com.ctrip.xpipe.redis.checker.resource.Resource.*;

/**
 * @author lishanglin
 * date 2021/3/8
 */
@Configuration
public class ResourceConfig extends AbstractRedisConfigContext {

    private final static int REDIS_SESSION_CLIENT_POOL_SIZE = Integer.parseInt(System.getProperty("REDIS_SESSION_CLIENT_POOL_SIZE", "12"));

    private final static int KEYED_CLIENT_POOL_SIZE = Integer.parseInt(System.getProperty("KEYED_CLIENT_POOL_SIZE", "8"));

    @Bean(name = REDIS_COMMAND_EXECUTOR)
    public ScheduledExecutorService getRedisCommandExecutor() {
        int corePoolSize = OsUtils.getCpuCount();
        if (corePoolSize > maxScheduledCorePoolSize) {
            corePoolSize = maxScheduledCorePoolSize;
        }
        return MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(corePoolSize, XpipeThreadFactory.create(REDIS_COMMAND_EXECUTOR)),
                THREAD_POOL_TIME_OUT, TimeUnit.SECONDS
        );
    }

    @Bean(name = KEYED_NETTY_CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getReqResNettyClientPool() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory(KEYED_CLIENT_POOL_SIZE));
        LifecycleHelper.initializeIfPossible(keyedObjectPool);
        LifecycleHelper.startIfPossible(keyedObjectPool);
        return keyedObjectPool;
    }

    @Bean(name = REDIS_SESSION_NETTY_CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getRedisSessionNettyClientPool() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory(REDIS_SESSION_CLIENT_POOL_SIZE));
        LifecycleHelper.initializeIfPossible(keyedObjectPool);
        LifecycleHelper.startIfPossible(keyedObjectPool);
        return keyedObjectPool;
    }

    @Bean(name = PING_DELAY_EXECUTORS)
    public ExecutorService getDelayPingExecturos() {
        return DefaultExecutorFactory.createAllowCoreTimeoutAbortPolicy("RedisHealthCheckInstance-").createExecutorService();
    }

    @Bean(name = PING_DELAY_SCHEDULED)
    public ScheduledExecutorService getDelayPingScheduled() {
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(Math.min(OsUtils.getCpuCount(), 4),
                XpipeThreadFactory.create("RedisHealthCheckInstance-Scheduled-"));
        ((ScheduledThreadPoolExecutor)scheduled).setRemoveOnCancelPolicy(true);
        ((ScheduledThreadPoolExecutor)scheduled).setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return scheduled;
    }

    private ProxyEnabledNettyKeyedPoolClientFactory getKeyedPoolClientFactory(int eventLoopThreads) {
        ProxyResourceManager resourceManager = new ConsoleProxyResourceManager(new NaiveNextHopAlgorithm());
        return new ProxyEnabledNettyKeyedPoolClientFactory(eventLoopThreads, resourceManager);
    }

}
