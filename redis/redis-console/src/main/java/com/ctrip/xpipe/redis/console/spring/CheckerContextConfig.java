package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.*;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.cluster.AllCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.config.impl.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info.RedisMsgCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.DefaultPingService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.SentinelMonitorsCheck;
import com.ctrip.xpipe.redis.checker.impl.*;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultCommonConfig;
import com.ctrip.xpipe.redis.console.healthcheck.meta.DcIgnoredConfigChangeListener;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultBeaconManager;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultMonitorManager;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.console.redis.DefaultSentinelManager;
import com.ctrip.xpipe.redis.console.resources.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.impl.DcClusterShardServiceImpl;
import com.ctrip.xpipe.redis.console.service.impl.DefaultDcRelationsService;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.console.service.meta.impl.BeaconMetaServiceImpl;
import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.core.route.impl.DefaultRouteChooseStrategyFactory;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;


/**
 * @author lishanglin
 * date 2021/3/8
 */
@Configuration
@ComponentScan(basePackages = { "com.ctrip.xpipe.redis.checker" })
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CHECKER)
public class CheckerContextConfig {

    @Bean
    public DcClusterShardService dcClusterShardService() {
        return new DcClusterShardServiceImpl();
    }

    @Bean
    public PersistenceCache persistenceCache(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        return new CheckerPersistenceCache(checkerConfig, checkerConsoleService);
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public MetaCache metaCache(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        return new CheckerMetaCache(checkerConfig, checkerConsoleService);
    }

    @Bean
    public CheckerAllMetaCache checkerAllMetaCache() {
        return new CheckerAllMetaCache();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_TEST)
    public MetaCache testMetaCache() {
        return new TestMetaCache();
    }


    @Bean
    public ProxyManager proxyManager(GroupCheckerLeaderElector clusterServer, CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        return new CheckerProxyManager(clusterServer, checkerConfig, checkerConsoleService);
    }

    @Bean
    public ConsoleCommonConfig consoleCommonConfig() {
        return new DefaultCommonConfig();
    }

    @Bean
    public CheckerConfig checkerConfig(CheckConfigBean checkConfigBean,
                                       ConsoleConfigBean consoleConfigBean,
                                       DataCenterConfigBean dataCenterConfigBean,
                                       CommonConfigBean commonConfigBean) {
        return new DefaultConsoleConfig(checkConfigBean, consoleConfigBean, dataCenterConfigBean, commonConfigBean);
    }

    @Bean
    public CheckerDbConfig checkerDbConfig(PersistenceCache persistenceCache) {
        return new DefaultCheckerDbConfig(persistenceCache);
    }

    @Bean
    public DcRelationsService dcRelationsService(){
        return new DefaultDcRelationsService();
    }

    @Bean
    public DcIgnoredConfigChangeListener dcIgnoredConfigChangeListener() {
        return new DcIgnoredConfigChangeListener();
    }

    @Bean
    public MetaServerManager metaServerManager(ConsoleConfig consoleConfig) {
        return new DefaultMetaServerConsoleServiceManagerWrapper(consoleConfig);
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public GroupCheckerLeaderElector checkerLeaderElector(FoundationService foundationService) {
        return new GroupCheckerLeaderElector(foundationService.getGroupId());
    }

    @Bean
    public CheckerRedisDelayManager redisDelayManager() {
        return new CheckerRedisDelayManager();
    }

    @Bean
    public CheckerRedisInfoManager redisInfoManager() {
        return new CheckerRedisInfoManager();
    }

    @Bean
    public DefaultPingService pingService() {
        return new DefaultPingService();
    }

    @Bean
    public ClusterHealthManager clusterHealthManager(@Qualifier(GLOBAL_EXECUTOR) ExecutorService executorService) {
        return new CheckerClusterHealthManager(executorService);
    }

    @Bean
    public MonitorManager monitorServiceManager(MetaCache metaCache, ConsoleConfig config, ConsoleCommonConfig consoleCommonConfig) {
        return new DefaultMonitorManager(metaCache, config, consoleCommonConfig);
    }

    @Bean
    public BeaconMetaService beaconMetaService(MetaCache metaCache, ConsoleCommonConfig config) {
        return new BeaconMetaServiceImpl(metaCache, config);
    }

    @Bean
    public BeaconManager beaconManager(MonitorManager monitorManager, BeaconMetaService beaconMetaService) {
        return new DefaultBeaconManager(monitorManager, beaconMetaService);
    }

    @Bean
    public CheckerCrossMasterDelayManager checkerCrossMasterDelayManager(FoundationService foundationService) {
        return new CheckerCrossMasterDelayManager(foundationService.getDataCenter());
    }

    @Bean
    public RemoteCheckerManager remoteCheckerManager(CheckerConfig checkerConfig, GroupCheckerLeaderElector checkerLeaderElector, MetaCache metaCache) {
        return new DefaultRemoteCheckerManager(checkerConfig, checkerLeaderElector, metaCache);
    }

    @Bean
    @Lazy
    public SentinelManager sentinelManager() {
        return new DefaultSentinelManager();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public HealthCheckReporter healthCheckReporter(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService,
                                                   GroupCheckerLeaderElector clusterServer, AllCheckerLeaderElector allCheckerLeaderElector, RedisDelayManager redisDelayManager,
                                                   CrossMasterDelayManager crossMasterDelayManager, PingService pingService,
                                                   ClusterHealthManager clusterHealthManager, List<HealthStateService> healthStateServices,
                                                   @Value("${server.port}") int serverPort) {
        return new HealthCheckReporter(healthStateServices, checkerConfig, checkerConsoleService, clusterServer, allCheckerLeaderElector, redisDelayManager,
                crossMasterDelayManager, pingService, clusterHealthManager, serverPort);
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public RedisMsgReporter keeperContainerInfoReporter(RedisMsgCollector redisMsgCollector,
                                                        CheckerConsoleService checkerConsoleService, CheckerConfig config) {
        return new RedisMsgReporter(redisMsgCollector, checkerConsoleService, config);
    }
    
    @Bean(name = "ALLCHECKER")
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public AllCheckerLeaderElector allCheckerLeaderElector(FoundationService foundationService) {
        return new AllCheckerLeaderElector(foundationService.getDataCenter());
    }

    @Bean
    public FoundationService foundationService() {
        return FoundationService.DEFAULT;
    }

    @Bean
    public SentinelMonitorsCheck sentinelMonitorsCheckCrossDc(CheckerAllMetaCache metaCache, PersistenceCache persistenceCache,
                                                              CheckerConfig config,
                                                              FoundationService foundationService,
                                                              SentinelManager manager,
                                                              AlertManager alertManager
                                                                     ) {
        return new SentinelMonitorsCheck(metaCache, persistenceCache, config, foundationService.getDataCenter(), manager, alertManager);
    }

    @Bean
    public OuterClientCache outerClientCache(CheckerConsoleService service, CheckerConfig config) {
        return new CheckerOuterClientCache(service, config);
    }

    @Bean
    public RouteChooseStrategyFactory getRouteChooseStrategyFactory() {
        return new DefaultRouteChooseStrategyFactory();
    }
}
