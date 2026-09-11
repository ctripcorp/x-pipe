package com.ctrip.xpipe.redis.comparator.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.comparator.balance.CmsServerGroupProvider;
import com.ctrip.xpipe.redis.comparator.balance.CompareTaskAssigner;
import com.ctrip.xpipe.redis.comparator.balance.ServerGroupProvider;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.concurrent.ScheduledExecutorService;

/**
 * 生产 profile 下注册外部依赖 Bean。测试 profile 不加载本类，单测注入
 * {@link ServerGroupProvider} 假实现，不访问网络（D23）。
 * 复制连接由 {@code KeeperReplStream} 自管，不在此注册 keyed client pool。
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Production extends AbstractProfile {

    @Bean
    public ServerGroupProvider serverGroupProvider(ComparatorConfig config) {
        return new CmsServerGroupProvider(config);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public CompareTaskAssigner compareTaskAssigner(ServerGroupProvider serverGroupProvider,
                                                   ComparatorConfig config,
                                                   @Qualifier(AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
                                                   ScheduledExecutorService scheduled) {
        return new CompareTaskAssigner(serverGroupProvider, config, FoundationService.DEFAULT, scheduled);
    }
}
