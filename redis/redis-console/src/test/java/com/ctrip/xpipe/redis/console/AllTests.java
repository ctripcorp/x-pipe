package com.ctrip.xpipe.redis.console;


import com.ctrip.xpipe.redis.console.alert.EmailSentCounterTest;
import com.ctrip.xpipe.redis.console.beacon.DefaultMonitorServiceManagerTest;
import com.ctrip.xpipe.redis.console.checker.DefaultCheckerManagerTest;
import com.ctrip.xpipe.redis.console.cluster.ConsoleCrossDcServerTest;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfigTest;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfigTest;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManagerTest;
import com.ctrip.xpipe.redis.console.controller.api.ChangeConfigTest;
import com.ctrip.xpipe.redis.console.controller.api.data.*;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckPrepareRequestTest;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfoTest;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisInstanceInfoTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.MigrationApiTest;
import com.ctrip.xpipe.redis.console.controller.config.ClusterCheckInterceptorTest;
import com.ctrip.xpipe.redis.console.controller.consoleportal.RedisControllerTest;
import com.ctrip.xpipe.redis.console.controller.consoleportal.RouteInfoControllerTest;
import com.ctrip.xpipe.redis.console.controller.consoleportal.migration.ExclusiveThreadsForMigrationTest;
import com.ctrip.xpipe.redis.console.dao.*;
import com.ctrip.xpipe.redis.console.election.CrossDcLeaderElectionActionTest;
import com.ctrip.xpipe.redis.console.healthcheck.NettyKeyedPoolClientFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.meta.DcIgnoredConfigListenerTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.availablezone.KeeperAvailableZoneCheckTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon.BeaconClusterMonitorCheckTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig.CheckClusterTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthStateTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl.DefaultClusterHealthMonitorManagerTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl.DefaultClusterHealthMonitorTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl.DefaultLeveledEmbededSetTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.AutoMigrationOffCheckerTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables.DBVariablesCheckTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables.checker.VariablesCheckerTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.metacache.MetaCacheCheckTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableCheckTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.redisconfig.RedisConfigCheckMonitorTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.sentinelconfig.SentinelConfigCheckTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.unhealthycluster.UnhealthyClusterCheckerTest;
import com.ctrip.xpipe.redis.console.migration.MigrationShardRollbackTest;
import com.ctrip.xpipe.redis.console.migration.MultiClusterMigrationTest;
import com.ctrip.xpipe.redis.console.migration.SingleShardMigrationTest;
import com.ctrip.xpipe.redis.console.migration.manager.DefaultMigrationEventManagerTest;
import com.ctrip.xpipe.redis.console.migration.model.DefaultMigrationClusterTest;
import com.ctrip.xpipe.redis.console.migration.model.DefaultMigrationShardTest;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultShardMigrationResultTest;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatTest;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatusTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationCheckingStateTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationInitiatedStateTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPartialSuccessStateTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPublishStatTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.statemachine.StateMachineTest;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTest;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifierTest;
import com.ctrip.xpipe.redis.console.notifier.DefaultClusterMonitorModifiedNotifierTest;
import com.ctrip.xpipe.redis.console.notifier.MetaNotifyTaskTest;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterTypeUpdateEventListenerTest;
import com.ctrip.xpipe.redis.console.proxy.ProxyPingRecorderTest;
import com.ctrip.xpipe.redis.console.proxy.impl.*;
import com.ctrip.xpipe.redis.console.resources.CheckerPersistenceCacheTest;
import com.ctrip.xpipe.redis.console.resources.DcMetaSynchronizerTest;
import com.ctrip.xpipe.redis.console.resources.DefaultMetaCacheTest;
import com.ctrip.xpipe.redis.console.resources.DefaultPersistenceCacheTest;
import com.ctrip.xpipe.redis.console.sentinel.impl.DefaultSentinelBalanceServiceTest;
import com.ctrip.xpipe.redis.console.service.BasicServiceTest;
import com.ctrip.xpipe.redis.console.service.MetaServiceTest;
import com.ctrip.xpipe.redis.console.service.ShardServiceTest2;
import com.ctrip.xpipe.redis.console.service.impl.*;
import com.ctrip.xpipe.redis.console.service.meta.impl.*;
import com.ctrip.xpipe.redis.console.service.migration.cmd.beacon.*;
import com.ctrip.xpipe.redis.console.service.migration.impl.BeaconMigrationServiceImplTest;
import com.ctrip.xpipe.redis.console.service.migration.impl.DefaultCheckMigrationCommandBuilderTest;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationServiceImplPaginationTest;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaBuilderTest;
import com.ctrip.xpipe.redis.console.spring.XPipeHandlerMethodCommandTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author wenchao.meng
 * <p>
 * Jun 23, 2016
 */
@RunWith(Suite.class)
@SuiteClasses(value = {
        ConsoleCrossDcServerTest.class,

        BasicServiceTest.class,
        ClusterServiceImplTest.class,
        ClusterServiceImplTest2.class,
        RedisServiceImplTest.class,
        KeeperContainerServiceImplTest.class,
        ShardServiceImplTest.class,
        ShardServiceImplTest2.class,
        ShardServiceTest2.class,
        SentinelServiceImplTest.class,
        ClusterMetaServiceImplTest.class,
        ClusterMetaServiceMigrationStatusChangeTest.class,
        DcServiceImplTest.class,
        ConfigServiceImplTest.class,
        AutoMigrationOffCheckerTest.class,
        AppliercontainerServiceImplTest.class,
        ReplDirectionServiceImplTest.class,
        ApplierServiceImplTest.class,

        StateMachineTest.class,
        MigrationStatusTest.class,
        ClusterMetaModifiedNotifierTest.class,
        MetaServiceTest.class,
        ClusterMetaModifiedNotifierTest.class,
        MetaNotifyTaskTest.class,
        MigrationCheckingStateTest.class,
        MigrationPartialSuccessStateTest.class,
        DefaultMigrationClusterTest.class,
        DefaultMigrationShardTest.class,
        MigrationStatTest.class,
        MigrationInitiatedStateTest.class,
        MigrationPublishStatTest.class,
        SingleShardMigrationTest.class,
        MigrationShardRollbackTest.class,
        DefaultShardMigrationResultTest.class,
        ClusterCreateInfoTest.class,
        ClusterServiceImplTest3.class,
        ClusterServiceImplTest4.class,

        CheckPrepareRequestTest.class,
        ConfigDaoTest.class,
        MigrationClusterDaoTest.class,
        MigrationEventDaoTest.class,
        DefaultConsoleDbConfigTest.class,
        DefaultConsoleConfigTest.class,
        RedisDaoTest.class,
        CheckClusterTest.class,

        RedisInstanceInfoTest.class,
        KeeperUpdateControllerTest.class,
        AzServiceImplTest.class,
        MetaUpdateTest.class,
        MetaUpdateTest2.class,
        MetaUpdateTest3.class,
        MetaUpdateTest4.class,

        ProxyDaoTest.class,
        RouteDaoTest.class,
        RouteServiceImplTest.class,
        RouteServiceImplTest.class,
        ProxyServiceImplTest.class,
        ProxyServiceImplTest2.class,

        AdvancedDcMetaServiceTestForRoute.class,
        AdvancedDcMetaServiceTest.class,
        ClusterMetaServiceImplTest.class,

        DcIgnoredConfigListenerTest.class,

        DefaultProxyChainTest.class,
        DefaultProxyMonitorCollectorManagerTest.class,
        DefaultProxyChainAnalyzerTest.class,
        ProxyPingRecorderTest.class,
        DcClusterShardTest.class,
        TestForAbstractMultiValueTunnelSocketStatsAnalyzer.class,
        TestForAbstractNormalKeyValueTunnelSocketStatsAnalyzer.class,
        DefaultTunnelSocketStatsAnalyzerManagerTest.class,
        TunnelSocketStatsAnalyzersTest.class,
        ClusterHealthStateTest.class,
        DefaultLeveledEmbededSetTest.class,
        DefaultClusterHealthMonitorTest.class,
        DefaultClusterHealthMonitorManagerTest.class,
        MigrationSystemAvailableCheckTest.class,
        DefaultCheckMigrationCommandBuilderTest.class,
        SentinelUpdateControllerTest.class,
        MetaCacheCheckTest.class,
        DefaultMigrationEventManagerTest.class,
        MigrationServiceImplPaginationTest.class,
        SentinelConfigCheckTest.class,
        DelayServiceTest.class,
        ShardMetaServiceImplTest.class,
        CrossDcLeaderElectionActionTest.class,
        ShardDaoTest.class,
        VariablesCheckerTest.class,
        DBVariablesCheckTest.class,
        DcMetaBuilderTest.class,
        DcClusterServiceImplTest.class,
        CrossMasterDelayServiceTest.class,
        DefaultMetaCacheTest.class,
        DcMetaSynchronizerTest.class,
        ConsoleServiceManagerTest.class,
        ChangeConfigTest.class,
        NettyKeyedPoolClientFactoryTest.class,
        UnhealthyClusterCheckerTest.class,
        ClusterCheckInterceptorTest.class,

        MigrationEventConcurrentCreateTest.class,
        BeaconMetaServiceImplTest.class,
        BeaconMigrationServiceImplTest.class,
        MigrationApiTest.class,
        ExclusiveThreadsForMigrationTest.class,
        XPipeHandlerMethodCommandTest.class,

        MigrationPreCheckCmdTest.class,
        MigrationFetchProcessingEventCmdTest.class,
        MigrationChooseTargetDcCmdTest.class,
        MigrationBuildEventCmdTest.class,
        MigrationDoExecuteCmdTest.class,
        MultiClusterMigrationTest.class,

        DefaultMonitorServiceManagerTest.class,
        BeaconClusterMonitorCheckTest.class,
        DefaultClusterMonitorModifiedNotifierTest.class,

        DefaultPersistenceCacheTest.class,
        CheckerPersistenceCacheTest.class,
        DefaultCheckerManagerTest.class,
        DefaultSentinelBalanceServiceTest.class,

        KeeperAvailableZoneCheckTest.class,
        RedisConfigCheckMonitorTest.class,
        RedisCheckRuleServiceImplTest.class,
        EmailSentCounterTest.class,

        ClusterTypeUpdateEventListenerTest.class,

        RouteInfoControllerTest.class,
        RedisControllerTest.class,
        DcRelationsServiceTest.class
})
public class AllTests {

}
