package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.cluster.ClusterServer;
import com.ctrip.xpipe.redis.checker.MetaServerManager;
import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.checker.cluster.CheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.config.impl.DefaultCheckerDbConfig;
import com.ctrip.xpipe.redis.checker.impl.CheckerMetaCache;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.meta.DcIgnoredConfigChangeListener;
import com.ctrip.xpipe.redis.console.resources.DefaultPersistence;
import com.ctrip.xpipe.redis.checker.impl.TestMetaCache;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.impl.DcClusterShardServiceImpl;
import com.ctrip.xpipe.redis.console.spring.condition.ConsoleServerMode;
import com.ctrip.xpipe.redis.console.spring.condition.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author lishanglin
 * date 2021/3/8
 */
@Configuration
@ComponentScan(basePackages = {"com.ctrip.xpipe.redis.checker", "com.ctrip.xpipe.redis.console.dao"})
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CHECKER)
public class CheckerContextConfig {

    @Bean
    public DcClusterShardService dcClusterShardService() {
        return new DcClusterShardServiceImpl();
    }

    @Bean(autowire = Autowire.BY_TYPE)
    public Persistence persistence() {
        return new DefaultPersistence();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public MetaCache metaCache() {
        return new CheckerMetaCache();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_TEST)
    public MetaCache testMetaCache() {
        return new TestMetaCache();
    }

    @Bean
    public DefaultConsoleConfig consoleConfig() {
        return new DefaultConsoleConfig();
    }

    @Bean
    public CheckerDbConfig checkerDbConfig(Persistence persistence) {
        return new DefaultCheckerDbConfig(persistence);
    }

    @Bean
    public DcIgnoredConfigChangeListener dcIgnoredConfigChangeListener() {
        return new DcIgnoredConfigChangeListener();
    }

    @Bean
    public MetaServerManager metaServerManager() {
        return new DefaultMetaServerConsoleServiceManagerWrapper();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public ClusterServer clusterServer() {
        return new CheckerLeaderElector();
    }

}
