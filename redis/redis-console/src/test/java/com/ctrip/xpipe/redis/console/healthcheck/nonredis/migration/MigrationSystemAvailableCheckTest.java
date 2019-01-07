package com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration;

import com.ctrip.xpipe.redis.console.AbstractConsoleH2DbTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MigrationSystemAvailableCheckTest extends AbstractConsoleH2DbTest {

    @Mock
    private DcService dcService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private AlertManager alertManager;

    @Mock
    private ConsoleConfig consoleConfig;

    @InjectMocks
    private DefaultMigrationSystemAvailableChecker check = new DefaultMigrationSystemAvailableChecker();

    @Before
    public void beforeMigrationSystemAvailableCheckTest() {
        MockitoAnnotations.initMocks(this);
        when(consoleConfig.getClusterShardForMigrationSysCheck()).thenReturn(Pair.from("cluster1", "shard1"));
        when(consoleConfig.getMetaservers()).thenReturn("{\"localmeta\": \"127.0.0.1:8080\"}");
    }

    @Test
    public void testInitBuilder() {
        check.doCheck();
        Assert.assertNotNull(check.getBuilder());
    }

    @Test
    public void testDoCheck() {
        check.doCheck();
        verify(dcService).findClusterRelatedDc(anyString());
        DefaultMigrationSystemAvailableChecker.MigrationSystemAvailability availability = check.getResult();
        Assert.assertFalse(availability.isAvaiable());
        logger.info("{}", availability.getMessage());
    }

    @Test
    public void testAlertTypes() {
        Assert.assertEquals(Lists.newArrayList(ALERT_TYPE.MIGRATION_SYSTEM_CHECK_OVER_DUE), check.alertTypes());
    }

    @Test
    public void testGetIntervalMilli() {
        Assert.assertEquals(30000, check.getIntervalMilli());
    }

    @Test
    public void testGetResult() {
        Assert.assertTrue(check.getResult().isAvaiable());
    }


}