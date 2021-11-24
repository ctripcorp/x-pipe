package com.ctrip.xpipe.redis.checker;

/**
 * @author lishanglin
 * date 2021/3/14
 */

import com.ctrip.xpipe.redis.checker.alert.AlertManagerTest;
import com.ctrip.xpipe.redis.checker.alert.manager.AlertPolicyManagerTest;
import com.ctrip.xpipe.redis.checker.alert.message.holder.DefaultAlertEntityHolderTest;
import com.ctrip.xpipe.redis.checker.cluster.monitor.DefaultSentinelMonitorsCheckTest;
import com.ctrip.xpipe.redis.checker.config.impl.DefaultCheckerDbConfigTest;
import com.ctrip.xpipe.redis.checker.controller.CheckerHealthControllerTest;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessageTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.MultiMasterDelayActionControllerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.MultiMasterDelayListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler.TestAbstractHealthEventHandlerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.diskless.DiskLessReplCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.version.VersionCheckActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.version.VersionCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.conflic.ConflictCheckActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.conflic.ConflictCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.conflic.ConflictMetricListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.BackStreamingAlertListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.CrdtInfoReplicationActionFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.CrdtInfoReplicationActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.expiresize.ExpireSizeCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.expiresize.ExpireSizeMetricListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tombstonesize.TombstoneSizeCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tombstonesize.TombstoneSizeMetricListenerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CurrentDcSentinelHelloAggregationCollectorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CurrentDcSentinelHelloCollectorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollectorTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.SentinelCollector4KeeperTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller.CurrentDcSentinelCheckControllerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconActiveDcControllerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconMetaCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.factory.DefaultHealthCheckEndpointFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.factory.DefaultHealthCheckInstanceFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.factory.HealthCheckEndpointFactoryTest;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckerMockTest;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.DefaultDcMetaChangeManagerTest;
import com.ctrip.xpipe.redis.checker.impl.*;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResultSerializeTest;
import com.ctrip.xpipe.redis.checker.resource.DefaultCheckerConsoleServiceTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
        DefaultCheckerDbConfigTest.class,

        DefaultSentinelHelloCollectorTest.class,
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

        DefaultAlertEntityHolderTest.class,
        DefaultDcMetaChangeManagerTest.class,
        DelayActionTest.class,
        MultiMasterDelayListenerTest.class,
        MultiMasterDelayActionControllerTest.class,
        CurrentDcSentinelHelloAggregationCollectorTest.class,
        CurrentDcSentinelHelloCollectorTest.class,
        CurrentDcSentinelCheckControllerTest.class,
        RedisMasterControllerTest.class,
        DefaultHealthCheckerMockTest.class,
        ConflictMetricListenerTest.class,
        ConflictCheckActionTest.class,
        ConflictCheckActionFactoryTest.class,
        ExpireSizeMetricListenerTest.class,
        ExpireSizeCheckActionTest.class,
        TombstoneSizeCheckActionTest.class,
        TombstoneSizeMetricListenerTest.class,
        DefaultRedisMasterActionListenerTest.class,

        BeaconMetaCheckActionTest.class,
        BeaconActiveDcControllerTest.class,

        SentinelHelloTest.class,
        HealthStatusTest.class,
        OuterClientServiceProcessorTest.class,

        CrdtInfoReplicationActionFactoryTest.class,
        CrdtInfoReplicationActionTest.class,
        BackStreamingAlertListenerTest.class,
        SentinelHelloCheckActionFactoryTest.class,
        SentinelHelloCheckActionTest.class,
        RouteHealthEventProcessorTest.class,
        SentinelHelloActionDowngradeTest.class,
        SentinelLeakyBucketTest.class,
        CurrentDcDelayPingActionCollectorTest.class,
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
        AlertManagerTest.class
})
public class AllTests {
}
