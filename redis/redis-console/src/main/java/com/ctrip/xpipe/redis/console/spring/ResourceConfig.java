package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.redis.core.client.NettyRedisPoolClientFactory;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private CheckerConfig consoleConfig;

    private final static int REDIS_SESSION_CLIENT_POOL_SIZE = Integer.parseInt(System.getProperty("REDIS_SESSION_CLIENT_POOL_SIZE", "12"));

    private final static int KEYED_CLIENT_POOL_SIZE = Integer.parseInt(System.getProperty("KEYED_CLIENT_POOL_SIZE", "8"));

    private final static String DEFAULT_CLIENT_NAME = "xpipe";

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

    @Bean(name = REDIS_KEYED_NETTY_CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getReqResNettyClientPool() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getRedisPoolClientFactory(KEYED_CLIENT_POOL_SIZE));
        LifecycleHelper.initializeIfPossible(keyedObjectPool);
        LifecycleHelper.startIfPossible(keyedObjectPool);
        return keyedObjectPool;
    }

    @Bean(name = REDIS_SESSION_NETTY_CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getRedisSessionNettyClientPool() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getRedisPoolClientFactory(REDIS_SESSION_CLIENT_POOL_SIZE));
        LifecycleHelper.initializeIfPossible(keyedObjectPool);
        LifecycleHelper.startIfPossible(keyedObjectPool);
        return keyedObjectPool;
    }

    @Bean(name = KEEPER_KEYED_NETTY_CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getKeeperReqResNettyClientPool() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory(KEYED_CLIENT_POOL_SIZE));
        LifecycleHelper.initializeIfPossible(keyedObjectPool);
        LifecycleHelper.startIfPossible(keyedObjectPool);
        return keyedObjectPool;
    }

    @Bean(name = SENTINEL_KEYED_NETTY_CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getSentinelReqResNettyClientPool() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory(KEYED_CLIENT_POOL_SIZE));
        LifecycleHelper.initializeIfPossible(keyedObjectPool);
        LifecycleHelper.startIfPossible(keyedObjectPool);
        return keyedObjectPool;
    }

    @Bean(name = PROXY_KEYED_NETTY_CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getProxyReqResNettyClientPool() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory(KEYED_CLIENT_POOL_SIZE));
        LifecycleHelper.initializeIfPossible(keyedObjectPool);
        LifecycleHelper.startIfPossible(keyedObjectPool);
        return keyedObjectPool;
    }

    @Bean(name = PING_DELAY_INFO_EXECUTORS)
    public ExecutorService getDelayPingExecturos() {
        return DefaultExecutorFactory.createAllowCoreTimeoutAbortPolicy("RedisHealthCheckInstance-").createExecutorService();
    }

    @Bean(name = PING_DELAY_INFO_SCHEDULED)
    public ScheduledExecutorService getDelayPingScheduled() {
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(Math.min(OsUtils.getCpuCount(), 4),
                XpipeThreadFactory.create("RedisHealthCheckInstance-Scheduled-"));
        ((ScheduledThreadPoolExecutor)scheduled).setRemoveOnCancelPolicy(true);
        ((ScheduledThreadPoolExecutor)scheduled).setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return scheduled;
    }

    @Bean(name = HELLO_CHECK_EXECUTORS)
    public ExecutorService getHelloCheckExecturos() {
        return DefaultExecutorFactory.createAllowCoreTimeoutAbortPolicy("XPipe-HelloCheck-").createExecutorService();
    }

    @Bean(name = HELLO_CHECK_SCHEDULED)
    public ScheduledExecutorService getHelloCheckScheduled() {
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(Math.min(OsUtils.getCpuCount(), 4),
                XpipeThreadFactory.create("XPipe-HelloCheck-Scheduled-"));
        ((ScheduledThreadPoolExecutor)scheduled).setRemoveOnCancelPolicy(true);
        ((ScheduledThreadPoolExecutor)scheduled).setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return scheduled;
    }

    private NettyKeyedPoolClientFactory getKeyedPoolClientFactory(int eventLoopThreads) {
        return new NettyKeyedPoolClientFactory(eventLoopThreads);
    }

    private NettyKeyedPoolClientFactory getRedisPoolClientFactory(int eventLoopThreads) {
        return new NettyRedisPoolClientFactory(eventLoopThreads, DEFAULT_CLIENT_NAME, () -> consoleConfig.getShouldDoAfterNettyClientConnected());
    }

}
