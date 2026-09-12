package com.ctrip.xpipe.redis.comparator.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.comparator.balance.CmsServerGroupProvider;
import com.ctrip.xpipe.redis.comparator.balance.CompareTaskAssigner;
import com.ctrip.xpipe.redis.comparator.balance.ServerGroupProvider;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.meta.ComparatorMetaService;
import com.ctrip.xpipe.redis.comparator.meta.KeeperStreamFactory;
import com.ctrip.xpipe.redis.comparator.meta.PrepareWatchCache;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager;
import com.ctrip.xpipe.redis.comparator.report.CompareMetricsCollector;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.comparator.report.DefaultCompareReporter;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 生产 profile 下注册外部依赖 Bean。测试 profile 不加载本类，单测注入
 * {@link ServerGroupProvider} / {@link PrepareWatchCache} / {@link KeeperStreamFactory}
 * 假实现、自行构造 {@link ComparatorMetaService}，不访问网络（D23）。
 * 复制连接由 {@code KeeperReplStream} 自管，不在此注册 keyed client pool。
 * 应用持有 {@link EventLoopGroup}，同一分片 N 路同一条 EventLoop（D32 ⑤）。
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Production extends AbstractProfile {

    public static final String COMPARATOR_EVENT_LOOP_GROUP = "comparatorEventLoopGroup";

    public static final String COMPARATOR_TASK_SCHEDULED = ShardCompareTaskManager.TASK_SCHEDULED;

    @Bean
    public ServerGroupProvider serverGroupProvider(ComparatorConfig config) {
        return new CmsServerGroupProvider(config);
    }

    @Bean
    public ComparatorMetaService comparatorMetaService(ComparatorConfig config) {
        return new ComparatorMetaService(config);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public CompareTaskAssigner compareTaskAssigner(ServerGroupProvider serverGroupProvider,
                                                   ComparatorConfig config,
                                                   @Qualifier(AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
                                                   ScheduledExecutorService scheduled) {
        return new CompareTaskAssigner(serverGroupProvider, config, FoundationService.DEFAULT, scheduled);
    }

    @Bean(name = COMPARATOR_TASK_SCHEDULED, destroyMethod = "shutdown")
    public ScheduledExecutorService comparatorTaskScheduled() {
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1,
                XpipeThreadFactory.create(COMPARATOR_TASK_SCHEDULED));
        exec.setRemoveOnCancelPolicy(true);
        return MoreExecutors.getExitingScheduledExecutorService(exec,
                AbstractSpringConfigContext.THREAD_POOL_TIME_OUT, TimeUnit.SECONDS);
    }

    @Bean(name = COMPARATOR_EVENT_LOOP_GROUP, destroyMethod = "shutdownGracefully")
    public EventLoopGroup comparatorEventLoopGroup() {
        int n = Math.min(Math.max(OsUtils.getCpuCount(), 1), 8);
        return new NioEventLoopGroup(n, XpipeThreadFactory.create("comparator-io", true));
    }

    @Bean
    public PrepareWatchCache prepareWatchCache(@Qualifier(COMPARATOR_EVENT_LOOP_GROUP) EventLoopGroup eventLoopGroup,
                                               @Qualifier(AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
                                               ScheduledExecutorService scheduled) {
        return new PrepareWatchCache.Default(eventLoopGroup, scheduled);
    }

    @Bean
    public KeeperStreamFactory keeperStreamFactory(@Qualifier(COMPARATOR_EVENT_LOOP_GROUP) EventLoopGroup eventLoopGroup,
                                                   @Qualifier(AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
                                                   ScheduledExecutorService scheduled,
                                                   ComparatorConfig config) {
        return new KeeperStreamFactory.Default(eventLoopGroup, scheduled, config,
                KeeperStreamFactory.DEFAULT_LISTENING_PORT);
    }

    @Bean
    public CompareReporter compareReporter(ComparatorConfig config) {
        return new DefaultCompareReporter(config);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public ShardCompareTaskManager shardCompareTaskManager(ComparatorMetaService comparatorMetaService,
                                                           CompareTaskAssigner compareTaskAssigner,
                                                           PrepareWatchCache prepareWatchCache,
                                                           KeeperStreamFactory keeperStreamFactory,
                                                           ComparatorConfig config,
                                                           CompareReporter compareReporter,
                                                           @Qualifier(COMPARATOR_TASK_SCHEDULED)
                                                           ScheduledExecutorService taskScheduled) {
        return new ShardCompareTaskManager(comparatorMetaService, compareTaskAssigner, prepareWatchCache,
                keeperStreamFactory, config, taskScheduled, EventMonitor.DEFAULT, compareReporter);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public CompareMetricsCollector compareMetricsCollector(ShardCompareTaskManager shardCompareTaskManager,
                                                           ComparatorConfig config,
                                                           @Qualifier(AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
                                                           ScheduledExecutorService scheduled) {
        return new CompareMetricsCollector(shardCompareTaskManager, scheduled, config);
    }
}
