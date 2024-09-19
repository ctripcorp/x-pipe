package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.CurrentDcAllMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

/**
 * @author lishanglin
 * date 2024/9/14
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultHealthCheckInstanceManagerTest extends AbstractCheckerTest {

    @InjectMocks
    private DefaultHealthChecker healthChecker;

    @InjectMocks
    private DefaultHealthCheckInstanceManager healthCheckInstanceManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private CurrentDcAllMeta currentDcAllMeta;

    @Mock
    private HealthCheckInstanceFactory instanceFactory;

    @Mock
    private RedisHealthCheckInstance mockCheckInstance;

    @Mock
    private KeeperHealthCheckInstance mockKeeperInstance;

    @Mock
    private ClusterHealthCheckInstance mockClusterInstance;

    @Before
    public void setupDefaultHealthCheckInstanceManagerTest() {
        Mockito.when(instanceFactory.create(Mockito.any(RedisMeta.class))).thenReturn(mockCheckInstance);
        Mockito.when(instanceFactory.create(Mockito.any(KeeperMeta.class))).thenReturn(mockKeeperInstance);
        Mockito.when(instanceFactory.create(Mockito.any(ClusterMeta.class))).thenReturn(mockClusterInstance);
        Mockito.when(instanceFactory.createRedisInstanceForAssignedAction(Mockito.any())).thenReturn(mockCheckInstance);
        Mockito.when(instanceFactory.getOrCreateRedisInstanceForPsubPingAction(Mockito.any())).thenReturn(mockCheckInstance);

        Mockito.when(checkerConfig.getIgnoredHealthCheckDc()).thenReturn(Collections.emptySet());

        Mockito.when(currentDcAllMeta.getCurrentDcAllMeta()).thenReturn(getXpipeMeta().findDc(FoundationService.DEFAULT.getDataCenter()));

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.doAnswer(inv -> {
            String currentDc = inv.getArgument(0, String.class);
            String otherDc = inv.getArgument(1, String.class);
            DcMeta currentDcMeta = getXpipeMeta().findDc(currentDc);
            DcMeta otherDcMeta = getXpipeMeta().findDc(otherDc);
            if (null == currentDcMeta || null == otherDcMeta) return false;
            return !currentDcMeta.getZone().equalsIgnoreCase(otherDcMeta.getZone());
        }).when(metaCache).isCrossRegion(Mockito.anyString(), Mockito.anyString());

        healthChecker.setInstanceManager(healthCheckInstanceManager);
    }

    @Test
    public void testInstanceMatch() {
        healthChecker.generateHealthCheckInstances();
        Assert.assertTrue(healthCheckInstanceManager.checkInstancesMiss(getXpipeMeta()));
    }

    @Test
    public void testInstanceDisMatch() {
        healthChecker.generateHealthCheckInstances();
        XpipeMeta xpipeMeta = getXpipeMeta();
        xpipeMeta.getDcs().get("jq").getClusters().remove("bbz_qmq_idempotent_fra_default");
        Assert.assertFalse(healthCheckInstanceManager.checkInstancesMiss(xpipeMeta));
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "multi-type-health-instances.xml";
    }


}
