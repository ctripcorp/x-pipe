package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Set;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public class BeaconMetaServiceImplTest extends AbstractConsoleIntegrationTest {

    private MetaCache metaCache;

    @Autowired
    private DcService dcService;

    @Autowired
    private ClusterMetaService clusterMetaService;

    private BeaconMetaServiceImpl beaconMetaService;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/beacon-migration-test.sql");
    }

    @Before
    public void setupBeaconMetaServiceImplTest() {
        metaCache = Mockito.mock(MetaCache.class);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.doAnswer(invocation -> {
            String activeDc = invocation.getArgumentAt(0, String.class);
            String backupDc = invocation.getArgumentAt(1, String.class);
            XpipeMeta xpipeMeta = getXpipeMeta();
            logger.info("[setupBeaconMetaServiceImplTest] {}", activeDc);
            return !xpipeMeta.getDcs().get(activeDc).getZone().equals(xpipeMeta.getDcs().get(backupDc).getZone());
        }).when(metaCache).isCrossRegion(Mockito.anyString(), Mockito.anyString());

        beaconMetaService = new BeaconMetaServiceImpl(metaCache, dcService, clusterMetaService);
    }

    @Test
    public void testBuildBeaconGroups() {
        Set<MonitorGroupMeta> groups = beaconMetaService.buildBeaconGroups("cluster1");
        logger.info("[testBuildBeaconGroups] {}", groups);
        Assert.assertEquals(expectedBeaconGroups(), groups);
    }

    @Test
    public void testCompareMetaWithXPipe() {
        Assert.assertTrue(beaconMetaService.compareMetaWithXPipe("cluster1", expectedBeaconGroups()));
    }

    @Test
    public void testBuildCurrentBeaconGroups() {
        Set<MonitorGroupMeta> groups = beaconMetaService.buildCurrentBeaconGroups("cluster1");
        logger.info("[testBuildCurrentBeaconGroups] {}", groups);
        Assert.assertEquals(expectedBeaconGroups(), groups);
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "multi-zone-meta.xml";
    }

    private Set<MonitorGroupMeta> expectedBeaconGroups() {
        return Sets.newHashSet(
                new MonitorGroupMeta("shard1+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6379"), HostPort.fromString("127.0.0.1:6380")), true),
                new MonitorGroupMeta("shard2+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6381"), HostPort.fromString("127.0.0.1:6382")), true),
                new MonitorGroupMeta("shard1+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6383"), HostPort.fromString("127.0.0.1:6384")), false),
                new MonitorGroupMeta("shard2+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6385"), HostPort.fromString("127.0.0.1:6386")), false)
        );
    }

}
