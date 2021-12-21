package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.cluster.AllCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.SentinelMonitorsCheckCrossDc;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.DefaultPingService;
import com.ctrip.xpipe.redis.checker.impl.CheckerRedisInfoManager;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.healthcheck.meta.DcIgnoredConfigChangeListener;
import com.ctrip.xpipe.redis.console.resources.DefaultPersistenceCache;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.RedisInfoService;
import com.ctrip.xpipe.redis.console.service.impl.AlertEventService;
import com.ctrip.xpipe.redis.console.service.impl.DefaultRedisInfoService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.*;

/**
 * @author lishanglin
 * date 2021/3/13
 */
@Configuration
@EnableAspectJAutoProxy
@ComponentScan(basePackages = {"com.ctrip.xpipe.service.sso", "com.ctrip.xpipe.redis.console", "com.ctrip.xpipe.redis.checker"})
@ServletComponentScan("com.ctrip.framework.fireman")
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CONSOLE_CHECKER)
public class ConsoleCheckerContextConfig extends ConsoleContextConfig {

    @Bean
    public DcIgnoredConfigChangeListener dcIgnoredConfigChangeListener() {
        return new DcIgnoredConfigChangeListener();
    }

    @Bean
    public DefaultPingService pingService() {
        return new DefaultPingService();
    }

    @Bean
    public CheckerRedisInfoManager redisInfoManager() {
        return new CheckerRedisInfoManager();
    }

    @Bean
    @Override
    public RedisInfoService redisInfoService() {
        return new DefaultRedisInfoService();
    }

    @Bean
    public PersistenceCache persistenceCache(CheckerConfig config,
                                        AlertEventService alertEventService,
                                        ConfigDao configDao,
                                        DcClusterShardService dcClusterShardService,
                                        RedisDao redisDao,
                                        ClusterDao clusterDao) {
        return new DefaultPersistenceCache(
                config,
                alertEventService,
                configDao,
                dcClusterShardService,
                redisDao,
                clusterDao);
    }

    @Bean(name = "ALLCHECKER")
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public AllCheckerLeaderElector allCheckerLeaderElector(FoundationService foundationService) {
        return new AllCheckerLeaderElector(foundationService.getDataCenter());
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public GroupCheckerLeaderElector checkerLeaderElector(FoundationService foundationService) {
        return new GroupCheckerLeaderElector(foundationService.getGroupId());
    }

    @Bean
    public FoundationService foundationService() {
        return FoundationService.DEFAULT;
    }

    @Bean
    public SentinelMonitorsCheckCrossDc sentinelMonitorsCheckCrossDc(MetaCache metaCache, PersistenceCache persistenceCache,
                                                                     CheckerConfig config,
                                                                     FoundationService foundationService,
                                                                     CheckerConsoleService service,
                                                                     SentinelManager manager,
                                                                     AlertManager alertManager
    ) {
        return new SentinelMonitorsCheckCrossDc(metaCache, persistenceCache, config, foundationService.getDataCenter(), manager, alertManager);
    }
}
