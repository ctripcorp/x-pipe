package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.beacon.BeaconGroupModel;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Set;

/**
 * @author lishanglin
 * date 2020/12/31
 */
@RunWith(MockitoJUnitRunner.class)
public class BeaconMetaServiceImplTest extends AbstractConsoleTest {

    @Mock
    private MetaCache metaCache;

    private BeaconMetaServiceImpl beaconMetaService;

    @Before
    public void setupBeaconMetaServiceImplTest() {
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.doAnswer(invocation -> {
            String activeDc = invocation.getArgumentAt(0, String.class);
            String backupDc = invocation.getArgumentAt(1, String.class);
            XpipeMeta xpipeMeta = getXpipeMeta();
            return !xpipeMeta.getDcs().get(activeDc).getZone().equals(xpipeMeta.getDcs().get(backupDc).getZone());
        }).when(metaCache).isCrossRegion(Mockito.anyString(), Mockito.anyString());

        beaconMetaService = new BeaconMetaServiceImpl(metaCache);
    }

    @Test
    public void testBuildBeaconGroups() {
        Set<BeaconGroupModel> groups = beaconMetaService.buildBeaconGroups("cluster1");
        logger.info("[testBuildBeaconGroups] {}", groups);
        Assert.assertEquals(2, groups.size());
        Assert.assertEquals(expectedBeaconGroups(), groups);
    }

    @Test
    public void testCompareMetaWithXPipe() {
        Assert.assertTrue(beaconMetaService.compareMetaWithXPipe("cluster1", expectedBeaconGroups()));
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "multi-zone-meta.xml";
    }

    private Set<BeaconGroupModel> expectedBeaconGroups() {
        return Sets.newHashSet(
                new BeaconGroupModel("shard1+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6379"), HostPort.fromString("127.0.0.1:6479")), true),
                new BeaconGroupModel("shard1+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:7379"), HostPort.fromString("127.0.0.1:7479")), false)
        );
    }

}
