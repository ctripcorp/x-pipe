package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/8/18
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultCrossMasterDelayServiceTest extends AbstractConsoleTest {

    @InjectMocks
    private DefaultCrossMasterDelayService crossMasterDelayService;

    @Mock
    private MetaCache metaCache;

    private String dc = FoundationService.DEFAULT.getDataCenter();

    @Override
    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }

    @Before
    public void setupDefaultCrossMasterDelayServiceTest() {
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
    }

    @Test
    public void testGetCurrentDcUnhealthyMasters() {
        crossMasterDelayService.updateCrossMasterDelays(new HashMap<DcClusterShard, Map<String, Pair<HostPort, Long>>>() {{
            put(new DcClusterShard(dc, "cluster1", "shard1"), Collections.singletonMap("oy", Pair.of(new HostPort("127.0.0.1", 8100), -1L)));
            put(new DcClusterShard(dc, "cluster2", "shard2"), Collections.singletonMap("oy", Pair.of(new HostPort("127.0.0.2", 8100), -1L)));
            put(new DcClusterShard(dc, "cluster3", "shard1"), Collections.singletonMap("oy", Pair.of(new HostPort("10.0.0.2", 6379), -1L)));
        }});

        UnhealthyInfoModel unhealthyInfoModel = crossMasterDelayService.getCurrentDcUnhealthyMasters();
        Assert.assertEquals(Collections.singleton("cluster3"), unhealthyInfoModel.getUnhealthyClusterNames());

        UnhealthyInfoModel expectedInfo = new UnhealthyInfoModel();
        expectedInfo.addUnhealthyInstance("cluster3", "jq", "shard1", new HostPort("10.0.0.1", 6379), true);
        Assert.assertEquals(expectedInfo.getUnhealthyInstance(), unhealthyInfoModel.getUnhealthyInstance());
    }

}
