package com.ctrip.xpipe.redis.checker;

/**
 * @author lishanglin
 * date 2021/3/14
 */

import com.ctrip.xpipe.redis.checker.alert.AlertManagerTest;
import com.ctrip.xpipe.redis.checker.alert.manager.AlertPolicyManagerTest;
import com.ctrip.xpipe.redis.checker.alert.message.holder.DefaultAlertEntityHolderTest;
import com.ctrip.xpipe.redis.checker.config.impl.DefaultCheckerDbConfigTest;
import com.ctrip.xpipe.redis.checker.controller.CheckerHealthControllerTest;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessageTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.crdtredisconf.CRDTRedisConfigCheckRuleActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.crdtredisconf.CRDTRedisConfigCheckRuleActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.CRDTDelayActionControllerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap.GtidGapCheckActionControllerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap.GtidGapCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.ClusterStatusAdjustCommandTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.InstanceHealthStatusCollectorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.InstanceHealthStatusConsistenceInspectorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.InstanceStatusAdjustCommandTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler.DefaultOuterClientAggregatorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler.TestAbstractHealthEventHandlerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.OuterClientServiceProcessorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route.DefaultRouteHealthEventProcessorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route.HeteroRouteHealthEventProcessorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionContextTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRuleTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRuleActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRuleActionListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRuleActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.diskless.DiskLessReplCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.version.VersionCheckActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.version.VersionCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.RedisMsgCollectorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.CrdtInfoReplicationActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.CrdtInfoReplicationActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.listener.BackStreamingAlertListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.listener.PeerBacklogOffsetListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.listener.PeerReplicationOffsetListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.CrdtInfoStatsActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.CrdtInfoStatsActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.listener.ConflictMetricListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.listener.CrdtSyncListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.expiresize.ExpireSizeCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.expiresize.ExpireSizeMetricListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication.InfoReplicationActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication.InfoReplicationActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication.RedisMasterBacklogOffsetListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication.RedisSlaveBacklogOffsetListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.infostats.InfoStatsActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.infostats.InfoStatsActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.infostats.RedisSnycListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tombstonesize.TombstoneSizeCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tombstonesize.TombstoneSizeMetricListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelLeakyBucketTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CurrentDcSentinelHelloCollectorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollectorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.SentinelCollector4KeeperTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command.AnalyseHellosTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command.CheckTrueMasterTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command.DeleteWrongSentinelsTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command.ResetSentinelsTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller.CrossDcSentinelHelloCheckControllerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller.CurrentDcSentinelHelloCheckControllerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller.OneWaySentinelHelloCheckControllerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.DefaultSentinelMonitorsCheckTest;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconConsistencyCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.DefaultBeaconMetaControllerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfigTest;
import com.ctrip.xpipe.redis.checker.healthcheck.factory.DefaultHealthCheckEndpointFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.factory.DefaultHealthCheckInstanceFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.factory.HealthCheckEndpointFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckInstanceManagerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckerMockTest;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRouteChooserTest;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.DefaultDcMetaChangeManagerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityInspectorTest;
import com.ctrip.xpipe.redis.checker.impl.*;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResultSerializeTest;
import com.ctrip.xpipe.redis.checker.resource.DefaultCheckerConsoleServiceTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
        DefaultCheckerDbConfigTest.class,

        DefaultSentinelHelloCollectorTest.class,
        AnalyseHellosTest.class,
        CheckTrueMasterTest.class,
        DeleteWrongSentinelsTest.class,
        ResetSentinelsTest.class,
        SentinelCollector4KeeperTest.class,
        AlertPolicyManagerTest.class,

        HealthCheckEndpointFactoryTest.class,
        DefaultHealthCheckEndpointFactoryTest.class,
        DefaultHealthCheckInstanceFactoryTest.class,

        TestAbstractHealthEventHandlerTest.class,
        VersionCheckActionTest.class,
        DiskLessReplCheckActionTest.class,
        RedisMasterCheckActionTest.class,
        RedisMasterCheckActionFactoryTest.class,
        RedisWrongSlaveMonitorTest.class,
        MasterOverOneMonitorTest.class,
        VersionCheckActionFactoryTest.class,
        RedisConfigCheckRuleActionFactoryTest.class,
        RedisConfigCheckRuleActionTest.class,
        CRDTRedisConfigCheckRuleActionFactoryTest.class,
        CRDTRedisConfigCheckRuleActionTest.class,
        RedisConfigCheckRuleActionListenerTest.class,
        RedisCheckRuleTest.class,

        DefaultAlertEntityHolderTest.class,
        DefaultDcMetaChangeManagerTest.class,
        DelayActionTest.class,
        CRDTDelayActionControllerTest.class,
        CurrentDcSentinelHelloCollectorTest.class,
        InfoReplicationActionTest.class,
        InfoReplicationActionFactoryTest.class,
        RedisMasterBacklogOffsetListenerTest.class,
        RedisSlaveBacklogOffsetListenerTest.class,
        InfoStatsActionFactoryTest.class,
        InfoStatsActionTest.class,
        RedisSnycListenerTest.class,

        RedisMasterControllerTest.class,
        DefaultHealthCheckerMockTest.class,
        ConflictMetricListenerTest.class,
        CrdtSyncListenerTest.class,
        CrdtInfoStatsActionTest.class,
        CrdtInfoStatsActionFactoryTest.class,
        ExpireSizeMetricListenerTest.class,
        ExpireSizeCheckActionTest.class,
        TombstoneSizeCheckActionTest.class,
        TombstoneSizeMetricListenerTest.class,
        DefaultRedisMasterActionListenerTest.class,
        CrossDcRedisMasterActionListenerTest.class,
        OutClientRedisMasterActionListenerTest.class,

        BeaconConsistencyCheckActionTest.class,
        DefaultBeaconMetaControllerTest.class,

        SentinelHelloTest.class,
        HealthStatusTest.class,
        HeteroHealthStatusTest.class,
        OuterClientServiceProcessorTest.class,
        InstanceHealthStatusCollectorTest.class,
        InstanceHealthStatusConsistenceInspectorTest.class,
        InstanceStatusAdjustCommandTest.class,
        ClusterStatusAdjustCommandTest.class,
        StabilityInspectorTest.class,

        PingActionContextTest.class,
        PingActionTest.class,

        CrdtInfoReplicationActionFactoryTest.class,
        CrdtInfoReplicationActionTest.class,
        BackStreamingAlertListenerTest.class,
        PeerBacklogOffsetListenerTest.class,
        PeerReplicationOffsetListenerTest.class,
        SentinelHelloCheckActionFactoryTest.class,
        SentinelHelloCheckActionTest.class,
        DefaultRouteHealthEventProcessorTest.class,
        HeteroRouteHealthEventProcessorTest.class,
        OneWaySentinelHelloCheckControllerTest.class,
        CurrentDcSentinelHelloCheckControllerTest.class,
        CrossDcSentinelHelloCheckControllerTest.class,

        SentinelLeakyBucketTest.class,
        CRDTDelayPingActionCollectorTest.class,
        ActionContextRetMessageTest.class,
        CheckerHealthControllerTest.class,

        CheckerClusterHealthManagerTest.class,
        CheckerCrossMasterDelayManagerTest.class,
        CheckerProxyManagerTest.class,
        CheckerRedisDelayManagerTest.class,
        DefaultRemoteCheckerManagerTest.class,

        HealthCheckResultSerializeTest.class,
        DefaultCheckerConsoleServiceTest.class,
        DefaultSentinelMonitorsCheckTest.class,
        AlertManagerTest.class,

        TestConnectProxyWithProxyClient.class,

        GtidGapCheckActionTest.class,
        GtidGapCheckActionControllerTest.class,

        DefaultHealthCheckConfigTest.class,
        DefaultRouteChooserTest.class,

        DefaultHealthCheckInstanceManagerTest.class,

        DefaultOuterClientAggregatorTest.class,
        DefaultDelayPingActionCollectorTest.class,
        DefaultAggregatorPullServiceTest.class,
        RedisMsgCollectorTest.class,
        RedisMsgReporterTest.class

})
public class AllTests {
}