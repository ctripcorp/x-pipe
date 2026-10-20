package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.cluster.AllCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.DefaultPingService;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.SentinelMonitorsCheck;
import com.ctrip.xpipe.redis.checker.impl.CheckerRedisInfoManager;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.resources.ConsoleMetaCacheWithoutDB;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.resources.DefaultMetaCache;
import com.ctrip.xpipe.redis.console.resources.DefaultPersistenceCache;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.RedisInfoService;
import com.ctrip.xpipe.redis.console.service.impl.AlertEventService;
import com.ctrip.xpipe.redis.console.service.impl.DefaultRedisInfoService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.*;

/**
 * @author lishanglin
 * date 2021/3/13
 */
@Configuration
@EnableAspectJAutoProxy
@ComponentScan(basePackages = {"com.ctrip.xpipe.service", "com.ctrip.xpipe.redis.console", "com.ctrip.xpipe.redis.checker"})
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CONSOLE_CHECKER)
public class ConsoleCheckerContextConfig extends ConsoleContextConfig {

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

    /**
     * CONSOLE_CHECKER 模式：嵌入 Checker 直接用 Console 的 MetaCache，不经 ConsoleCheckerController 出口填充。
     * 在 loadCache 时就地填充容器 Keeper IP，使同进程 Checker 组件拿到真实 IP。
     */
    @Bean
    @Override
    @Lazy
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public MetaCache metaCache(ConsolePortalService consolePortalService, ConsoleConfig config) {
        DefaultMetaCache cache = config.disableDb()
                ? new ConsoleMetaCacheWithoutDB(consolePortalService, config)
                : new DefaultMetaCache();
        cache.fillKeeperIpInCache = true;
        return cache;
    }

    @Bean
    public SentinelMonitorsCheck sentinelMonitorsCheckCrossDc(MetaCache metaCache, PersistenceCache persistenceCache,
                                                              CheckerConfig config,
                                                              FoundationService foundationService,
                                                              SentinelManager manager,
                                                              AlertManager alertManager
    ) {
        return new SentinelMonitorsCheck(metaCache, persistenceCache, config, foundationService.getDataCenter(), manager, alertManager);
    }

}
