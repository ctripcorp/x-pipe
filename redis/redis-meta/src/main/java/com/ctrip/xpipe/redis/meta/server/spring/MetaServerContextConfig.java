package com.ctrip.xpipe.redis.meta.server.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadMutexableTaskExecutor;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.spring.DomainValidateFilter;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * @author marsqing
 *         <p>
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan(basePackages = {"com.ctrip.xpipe.redis.meta.server"})
public class MetaServerContextConfig extends AbstractRedisConfigContext {

    @Autowired
    private MetaServerConfig metaServerConfig;

    public static final String CLIENT_POOL = "clientPool";

    public static final String REPLICATION_ADJUST_EXECUTOR = "replicationAdjustExecutor";
    public static final int REPLICATION_ADJUST_THREAD = 100;
    public static final int REPLICATION_ADJUST_THREAD_MAX = 100;

    public static final String REPLICATION_ADJUST_SCHEDULED = "replicationAdjustScheduled";

    @Bean(name = CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getClientPool() {

        return new XpipeNettyClientKeyedObjectPool();
    }

    @Bean(name = REPLICATION_ADJUST_EXECUTOR)
    public ExecutorService getReplicationAdjustExecutor() {
        DefaultExecutorFactory executorFactory = new DefaultExecutorFactory(REPLICATION_ADJUST_EXECUTOR,
                REPLICATION_ADJUST_THREAD, REPLICATION_ADJUST_THREAD_MAX, new ThreadPoolExecutor.AbortPolicy());
        return executorFactory.createExecutorService();
    }

    @Bean(name = REPLICATION_ADJUST_SCHEDULED)
    public ScheduledExecutorService getReplicationAdjustScheduled() {

        int corePoolSize = Math.min(OsUtils.getCpuCount(), maxScheduledCorePoolSize);
        return MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(corePoolSize, XpipeThreadFactory.create(REPLICATION_ADJUST_SCHEDULED)),
                THREAD_POOL_TIME_OUT, TimeUnit.SECONDS
        );
    }

    @Bean(name = CLUSTER_SHARD_ADJUST_EXECUTOR)
    public KeyedOneThreadMutexableTaskExecutor<Pair<String, String>> getClusterShardAdjustExecutor(
            @Qualifier(REPLICATION_ADJUST_EXECUTOR) ExecutorService executors, @Qualifier(REPLICATION_ADJUST_SCHEDULED) ScheduledExecutorService scheduled) {
        return new KeyedOneThreadMutexableTaskExecutor<>(executors, scheduled);
    }

    @Bean(name = PEER_MASTER_CHOOSE_EXECUTOR)
    public KeyedOneThreadTaskExecutor<Pair<String, String> > getPeerMasterChooseExecutor(
            @Qualifier(GLOBAL_EXECUTOR) ExecutorService executors) {
        return new KeyedOneThreadTaskExecutor<>(executors);
    }

    @Bean(name = PEER_MASTER_ADJUST_EXECUTOR)
    public KeyedOneThreadTaskExecutor<Pair<String, String> > getPeerMasterAdjustExecutor(
            @Qualifier(GLOBAL_EXECUTOR) ExecutorService executors) {
        return new KeyedOneThreadTaskExecutor<>(executors);
    }

    @Bean
    public MetaServerMultiDcServiceManager getMetaServerMultiDcServiceManager() {

        return new DefaultMetaServerMultiDcServiceManager();
    }

    @Bean
    public FilterRegistrationBean<DomainValidateFilter> domainValidateFilter() {
        FilterRegistrationBean<DomainValidateFilter> registrationBean = new FilterRegistrationBean<>();
        Supplier<String> expectedDomainName = () -> {
            // toLowerCase() to match metaServerConfig retrieve info
            String dcName = FoundationService.DEFAULT.getDataCenter().toLowerCase();
            DcInfo dcInfo = metaServerConfig.getDcInofs().get(dcName);
            return dcInfo.getMetaServerAddress();
        };
        DomainValidateFilter filter = new DomainValidateFilter(()->metaServerConfig.validateDomain(), expectedDomainName);
        registrationBean.setFilter(filter);
        registrationBean.addUrlPatterns("/*");
        return registrationBean;
    }
}
