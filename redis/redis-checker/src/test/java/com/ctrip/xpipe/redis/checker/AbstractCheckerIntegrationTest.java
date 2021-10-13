package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.config.impl.DefaultCheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.DefaultPingService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.impl.CheckerClusterHealthManager;
import com.ctrip.xpipe.redis.checker.impl.CheckerRedisInfoManager;
import com.ctrip.xpipe.redis.checker.impl.DefaultRemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.impl.TestMetaCache;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.netty.ProxyEnabledNettyKeyedPoolClientFactory;
import com.ctrip.xpipe.redis.core.proxy.resource.ConsoleProxyResourceManager;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.*;

import static com.ctrip.xpipe.redis.checker.resource.Resource.*;

/**
 * @author lishanglin
 * date 2021/3/9
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = AbstractCheckerIntegrationTest.CheckerTestConfig.class)
public class AbstractCheckerIntegrationTest extends AbstractCheckerTest {

    protected String[] dcNames = new String[]{"jq", "oy"};

    public static final String CHECKER_TEST = "xpipe.checker.test";

    @BeforeClass
    public static void beforeAbstractCheckerIntegrationTest(){
        System.setProperty(HealthChecker.ENABLED, "false");
        System.setProperty(CHECKER_TEST, "true");
    }

    @SpringBootApplication
    @ConditionalOnProperty(name = { CHECKER_TEST })
    public static class CheckerTestConfig extends AbstractRedisConfigContext {

        @Bean
        public MetaCache metaCache() {
            return new TestMetaCache();
        }

        @Bean
        public TestPersistenceCache persistence() {
            return new TestPersistenceCache();
        }

        @Bean
        public TestConfig testConfig() {
            return new TestConfig();
        }

        @Bean
        public CheckerDbConfig checkerDbConfig(PersistenceCache persistenceCache) {
            return new DefaultCheckerDbConfig(persistenceCache);
        }

        @Bean
        public MetaServerManager metaServerManager() {
            return new TestMetaServerManager();
        }

        @Bean
        public SentinelManager sentinelManager() {
            return new TestSentinelManager();
        }

        @Bean
        public PingService pingService() {
            return new DefaultPingService();
        }

        @Bean
        public CheckerRedisInfoManager redisInfoManager() {
            return new CheckerRedisInfoManager();
        }

        @Bean
        public RemoteCheckerManager remoteCheckerManager(CheckerConfig checkerConfig) {
            return new DefaultRemoteCheckerManager(checkerConfig);
        }

        @Bean
        public ClusterHealthManager clusterHealthManager(@Qualifier(GLOBAL_EXECUTOR) ExecutorService executorService) {
            return new CheckerClusterHealthManager(executorService);
        }

        @Bean
        public BeaconManager beaconManager() {
            return new TestBeaconManager();
        }

        @Bean(name = REDIS_COMMAND_EXECUTOR)
        public ScheduledExecutorService getRedisCommandExecutor() {
            return MoreExecutors.getExitingScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(2, XpipeThreadFactory.create(REDIS_COMMAND_EXECUTOR)),
                    THREAD_POOL_TIME_OUT, TimeUnit.SECONDS
            );
        }

        @Bean(name = KEYED_NETTY_CLIENT_POOL)
        public XpipeNettyClientKeyedObjectPool getReqResNettyClientPool() throws Exception {
            XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory(8));
            LifecycleHelper.initializeIfPossible(keyedObjectPool);
            LifecycleHelper.startIfPossible(keyedObjectPool);
            return keyedObjectPool;
        }

        @Bean(name = REDIS_SESSION_NETTY_CLIENT_POOL)
        public XpipeNettyClientKeyedObjectPool getRedisSessionNettyClientPool() throws Exception {
            XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory(12));
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
            ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(2,
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

        private ProxyEnabledNettyKeyedPoolClientFactory getKeyedPoolClientFactory(int eventLoopThreads) {
            ProxyResourceManager resourceManager = new ConsoleProxyResourceManager(new NaiveNextHopAlgorithm());
            return new ProxyEnabledNettyKeyedPoolClientFactory(eventLoopThreads, resourceManager);
        }
        
        @Bean
        public FoundationService foundationService() {
            return FoundationService.DEFAULT;
        }

    }

}
