package com.ctrip.xpipe.redis.console.healthcheck.nonredis.redisconfig;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.service.impl.DcClusterServiceImpl;
import com.ctrip.xpipe.redis.console.service.impl.DcServiceImpl;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisConfigCheckMonitorTest {
    @InjectMocks
    RedisConfigCheckMonitor redisConfigCheckMonitor;

    @Mock
    MetaCache metaCache;

    @Mock
    DcClusterServiceImpl dcClusterService;

    @Mock
    DcServiceImpl dcService;

    @Mock
    DefaultConsoleConfig consoleConfig;


    private ClusterType mockClusterType = ClusterType.BI_DIRECTION;

    private List<String> mockDcs = Arrays.asList("jq", "oy", "fra");

    private List<String> mockClusters = Arrays.asList("cluster1", "cluster2");


    private Map<String, Long> dcNameZoneMap = new HashMap<>();

    @Before
    public void beforeKeeperAvailableZoneCheckTest() {
        when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
        when(dcService.dcNameZoneMap()).thenReturn(dcNameZoneMap);
        when(consoleConfig.isRedisConfigCheckMonitorOpen()).thenReturn(true);
        when(dcClusterService.findDcClusterCreateInfo(anyString(), anyString()))
                .thenReturn(new DcClusterCreateInfo().setRedisCheckRule("1,2").setClusterName("cluster1").setDcName("fra"));

        dcNameZoneMap.put("jq", 1L);
        dcNameZoneMap.put("oy", 1L);
        dcNameZoneMap.put("fra", 2L);
    }

    @Test
    public void testDoCheck() {
        when(consoleConfig.getRedisConfigCheckRules()).thenReturn("2,3");
        redisConfigCheckMonitor.doCheck();
        verify(dcClusterService, times(6)).updateDcCluster(any(DcClusterCreateInfo.class));
    }

    @Test
    public void testDoCheckWithNullRedisConfigCheckRule() {
        when(consoleConfig.getRedisConfigCheckRules()).thenReturn(null);
        redisConfigCheckMonitor.doCheck();
        verify(dcClusterService, times(0)).updateDcCluster(any(DcClusterCreateInfo.class));
    }

    @Test
    public void testDoCheckWithNonRedisConfigCheckRule() {
        when(consoleConfig.getRedisConfigCheckRules()).thenReturn("");
        redisConfigCheckMonitor.doCheck();
        verify(dcClusterService, times(0)).updateDcCluster(any(DcClusterCreateInfo.class));
    }

    @Test
    public void testIsBiDirectionAndCrossRegionCluster() {
        ClusterMeta clusterMeta = new ClusterMeta().setType(ClusterType.BI_DIRECTION.name()).setActiveRedisCheckRules("1,2").setDcs("jq,oy,fra");
        Assert.assertEquals(true, redisConfigCheckMonitor.isBiDirectionAndCrossRegionCluster(clusterMeta, dcNameZoneMap));

        clusterMeta.setType(ClusterType.ONE_WAY.name());
        Assert.assertEquals(false, redisConfigCheckMonitor.isBiDirectionAndCrossRegionCluster(clusterMeta, dcNameZoneMap));

        clusterMeta.setType(ClusterType.BI_DIRECTION.name()).setDcs("jq,oy");
        Assert.assertEquals(false, redisConfigCheckMonitor.isBiDirectionAndCrossRegionCluster(clusterMeta, dcNameZoneMap));
    }

    @Test
    public void testGenerateNewRedisConfigRule() {
        Assert.assertEquals(redisConfigCheckMonitor.generateNewRedisConfigCheckRule("1,2", "2,3"),"1,2,3");
        Assert.assertEquals(redisConfigCheckMonitor.generateNewRedisConfigCheckRule(null, "2,3"),"2,3");
        Assert.assertEquals(redisConfigCheckMonitor.generateNewRedisConfigCheckRule("", "2,3"),"2,3");
    }


    private XpipeMeta mockXpipeMeta() {
        XpipeMeta meta = new XpipeMeta();

        for (String dc: mockDcs) {
            meta.addDc(mockDcMeta(dc));
        }

        meta.addRedisCheckRule(new RedisCheckRuleMeta(1L).setCheckType("config").setParam("{ 'configName' : 'repl_backlog_size', 'expectedVaule' : '256' }"));
        meta.addRedisCheckRule(new RedisCheckRuleMeta(2L).setCheckType("info").setParam("{ 'configName' : 'repl_backlog_size', 'expectedVaule' : '128' }"));

        return meta;
    }

    private DcMeta mockDcMeta(String dc) {
        DcMeta dcMeta = new DcMeta();
        dcMeta.setId(dc);

        for (String cluster: mockClusters) {
            dcMeta.addCluster(mockClusterMeta(cluster));
        }

        return dcMeta;
    }

    private ClusterMeta mockClusterMeta(String cluster) {
        ClusterMeta clusterMeta = new ClusterMeta();
        clusterMeta.setId(cluster);
        clusterMeta.setType(mockClusterType.toString());
        clusterMeta.setActiveRedisCheckRules("1,3");
        clusterMeta.setDcs("jq,oy,fra");

        return clusterMeta;
    }
}
